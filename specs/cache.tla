---- MODULE cache ----
EXTENDS TLC, Integers, Sequences, FiniteSets

CONSTANT 
    NULL,
    MaxSeries,
    MaxEpochs,
    Delay,
    Ingesters,
    BgWorkers
ASSUME 
    /\ MaxSeries \in Nat 
    /\ MaxEpochs \in Nat
    /\ Delay \in 0..(MaxEpochs - 1)
    /\ Cardinality(BgWorkers) = 1
    /\ Cardinality(Ingesters) > 0
    /\ BgWorkers \intersect Ingesters = {}

SeriesId == 1..MaxSeries
Epoch == 0..MaxEpochs
SeriesEntry ==
    [
        marked_at: Epoch \union {NULL},
        stored: BOOLEAN \* In TLA function sets are total, therefore we need an explicit flag
    ]
NoEntry == [marked_at |-> NULL, stored |-> FALSE]
NewEntry == [marked_at |-> NULL, stored |-> TRUE]
EntryMarkedForDeletion(e) == [marked_at |-> e, stored |-> TRUE]

Max(S) == CHOOSE x \in S : \A y \in S : x >= y
Min(S) == CHOOSE x \in S : \A y \in S : x =< y

(*--algorithm cache
variables
    now = 0;
    current_epoch = 0;
    delete_epoch = 0;
    series_metadata = [x \in SeriesId |-> NoEntry];
    series_referenced_from_data = {};
    cached_series = [i \in Ingesters |-> {}];
    observed_epochs = [i \in Ingesters |-> 0];

define
    TypeInvariant == 
        /\ now \in Epoch
        /\ current_epoch \in Epoch
        /\ delete_epoch \in Epoch
        /\ series_metadata \in [SeriesId -> SeriesEntry]
        /\ series_referenced_from_data \in SUBSET SeriesId
        /\ cached_series \in [Ingesters -> SUBSET SeriesId]
        /\ observed_epochs \in [Ingesters -> Epoch]

    ForeignKeySafety ==
        /\ \A sid \in series_referenced_from_data: series_metadata[sid].stored = TRUE

    StoredSeries == 
        {s \in DOMAIN series_metadata: series_metadata[s].stored = TRUE}

    MarkedSeries == 
        {s \in DOMAIN series_metadata: series_metadata[s].marked_at # NULL}

    MarkedAndRipe(e) == 
        {s \in MarkedSeries: (series_metadata[s].marked_at) < e }

    NewDataAfterMarked ==
        {s \in MarkedSeries: s \in series_referenced_from_data}

end define;

process cache_refresh_worker = 1 \* TODO
variables 
    fetched_epoch = 0;
    fetched_series = {};
begin
    SelectFromDb:
        (* SELECT id FROM _prom_catalog.series WHERE delete_at IS NOT NULL *)
        fetched_series := MarkedSeries;
        (* 
         * Should use SELECT FOR SHARE when fetching current_epoch,
         * To ensure a concurrent Mark worker didn't make changes.
         *)
        fetched_epoch := current_epoch;
    ScrubCache:
        with i \in Ingesters do 
            (* This needs to be under an RW mutex *)
            observed_epochs[i] := fetched_epoch;
            cached_series[i] := cached_series[i] \ fetched_series;
        end with;
end process;

process ingester \in Ingesters
variables 
    new_series = {};
    new_references = {};
    locally_observed_epoch = 0;
begin
    IngesterBegin:
        either
            ReceiveInput:
                (*
                * Technically multiple series could be ingested at once.
                * This spec should achieve an equivalent behaviour by
                * passing through IngesterBegin multiple times in a row.
                *)
                with series \in SeriesId do 
                    new_series := { series };
                end with;
            CacheLookupTransaction:
                (* The cache RW mutex must be held during this transaction *)
                new_references := new_series;
                new_series := new_series \ cached_series[self];
                (* 
                 * get_or_create_series_id calls ressurect_series_id 
                 * which, in turn, uses UPDATE and will cause a transaction
                 * conflict with a simultaneous deletion attempt.
                 *)
                series_metadata := [x \in new_series |-> NewEntry] @@ series_metadata;
                \* TODO
                \* The previous line is actually a DB transaction that could
                \* be rolled back. We need to re-model subsequent cache update
                \* more thoroughly.
                cached_series[self] := cached_series[self] \union new_series;
                locally_observed_epoch := observed_epochs[self];
            IngestTransaction:
                \* this if is epoch_abort and should be extracted and named
                if locally_observed_epoch <= delete_epoch then 
                    skip;
                else
                    series_referenced_from_data := series_referenced_from_data \union new_references;
                end if;
                goto IngesterBegin;
        or
            (* Ingester is done *)
            skip;
        end either;
end process;        

process bg_worker \in BgWorkers
variables
    candidates = {};
begin
    (* 
     * Under Read Commited each statement might see slightly different state.
     * It is modelled by confining each statement to its own label/action.
     *)
    BgWorkerTxBegin:
        while now < MaxEpochs do
            (* When a backgroud worker starts a transaction, sometimes some time has passed. *)
            with dt \in 0..1 do
                now := now + dt;
            end with;
            (* 
             * In fact, drop_metric_chunks goes through these stages sequentially. So, this actually
             * adds a touch more concurrency. But we assume concurrent phases mutually exclude
             * by grabbing a lock.
             *)
            either
                (* drop_metric_chunk_data *)
                DropChunkData:
                    (*
                     * Technically multiple series could be dropped at once.
                     * This spec is supposed to achieve this by entering 
                     * this branch of either without advancing now.
                     *)
                    with series_to_drop \in SeriesId do 
                        series_referenced_from_data := series_referenced_from_data \ {series_to_drop};
                    end with;
            or
                (* mark_series_to_be_dropped_as_unused *)
                AdvanceEpoch:
                    current_epoch := Max({current_epoch, now});
                    (* In this spec nothing else modifes current_epoch, so it's safe to not cache the value *)
                MarkUnused:
                    with stale_series = {s \in StoredSeries: s \notin series_referenced_from_data} do 
                        series_metadata := 
                            [s \in stale_series |-> EntryMarkedForDeletion(current_epoch)] @@ series_metadata;
                    end with;
            or
                (* delete_expired_series *)
                PrepareDeleteTx:
                    (* This requires an UPDATE lock on delete_epoch, and current_epoch *)
                    delete_epoch := Max({current_epoch - Delay, delete_epoch});
                    (* In this spec nothing else modifes delete_epoch, so it's safe to not cache the value *)
                    (* First COMMIT *)
                ActuallyDeleteTx:
                    (*
                     * DELETE ... RETURNING id statment. Otherwise
                     * under Read Commited it could stomp over a client-driven
                     * ressurect-UPDATE.
                     * (Alternatively, rechecking WHERE conditions should work)
                     *
                     * DELETE should provoke a conflict with
                     * get_or_create_series_id called by Ingester
                     *)
                    candidates := MarkedAndRipe(delete_epoch) \ NewDataAfterMarked; 
                    series_metadata := [x \in candidates |-> NoEntry] @@ series_metadata;
                Ressurect:
                    series_metadata := 
                        [x \in (MarkedAndRipe(delete_epoch) \ candidates) |-> NewEntry] @@ series_metadata;
                    (* Second COMMIT *)
            end either;
        end while;
end process;        
end algorithm; *)
    
\* BEGIN TRANSLATION (chksum(pcal) = "71dcf6a0" /\ chksum(tla) = "b5165707")
VARIABLES now, current_epoch, delete_epoch, series_metadata, 
          series_referenced_from_data, cached_series, observed_epochs, pc

(* define statement *)
TypeInvariant ==
    /\ now \in Epoch
    /\ current_epoch \in Epoch
    /\ delete_epoch \in Epoch
    /\ series_metadata \in [SeriesId -> SeriesEntry]
    /\ series_referenced_from_data \in SUBSET SeriesId
    /\ cached_series \in [Ingesters -> SUBSET SeriesId]
    /\ observed_epochs \in [Ingesters -> Epoch]

ForeignKeySafety ==
    /\ \A sid \in series_referenced_from_data: series_metadata[sid].stored = TRUE

StoredSeries ==
    {s \in DOMAIN series_metadata: series_metadata[s].stored = TRUE}

MarkedSeries ==
    {s \in DOMAIN series_metadata: series_metadata[s].marked_at # NULL}

MarkedAndRipe(e) ==
    {s \in MarkedSeries: (series_metadata[s].marked_at) < e }

NewDataAfterMarked ==
    {s \in MarkedSeries: s \in series_referenced_from_data}

VARIABLES fetched_epoch, fetched_series, new_series, new_references, 
          locally_observed_epoch, candidates

vars == << now, current_epoch, delete_epoch, series_metadata, 
           series_referenced_from_data, cached_series, observed_epochs, pc, 
           fetched_epoch, fetched_series, new_series, new_references, 
           locally_observed_epoch, candidates >>

ProcSet == {1} \cup (Ingesters) \cup (BgWorkers)

Init == (* Global variables *)
        /\ now = 0
        /\ current_epoch = 0
        /\ delete_epoch = 0
        /\ series_metadata = [x \in SeriesId |-> NoEntry]
        /\ series_referenced_from_data = {}
        /\ cached_series = [i \in Ingesters |-> {}]
        /\ observed_epochs = [i \in Ingesters |-> 0]
        (* Process cache_refresh_worker *)
        /\ fetched_epoch = 0
        /\ fetched_series = {}
        (* Process ingester *)
        /\ new_series = [self \in Ingesters |-> {}]
        /\ new_references = [self \in Ingesters |-> {}]
        /\ locally_observed_epoch = [self \in Ingesters |-> 0]
        (* Process bg_worker *)
        /\ candidates = [self \in BgWorkers |-> {}]
        /\ pc = [self \in ProcSet |-> CASE self = 1 -> "SelectFromDb"
                                        [] self \in Ingesters -> "IngesterBegin"
                                        [] self \in BgWorkers -> "BgWorkerTxBegin"]

SelectFromDb == /\ pc[1] = "SelectFromDb"
                /\ fetched_series' = MarkedSeries
                /\ fetched_epoch' = current_epoch
                /\ pc' = [pc EXCEPT ![1] = "ScrubCache"]
                /\ UNCHANGED << now, current_epoch, delete_epoch, 
                                series_metadata, series_referenced_from_data, 
                                cached_series, observed_epochs, new_series, 
                                new_references, locally_observed_epoch, 
                                candidates >>

ScrubCache == /\ pc[1] = "ScrubCache"
              /\ \E i \in Ingesters:
                   /\ observed_epochs' = [observed_epochs EXCEPT ![i] = fetched_epoch]
                   /\ cached_series' = [cached_series EXCEPT ![i] = cached_series[i] \ fetched_series]
              /\ pc' = [pc EXCEPT ![1] = "Done"]
              /\ UNCHANGED << now, current_epoch, delete_epoch, 
                              series_metadata, series_referenced_from_data, 
                              fetched_epoch, fetched_series, new_series, 
                              new_references, locally_observed_epoch, 
                              candidates >>

cache_refresh_worker == SelectFromDb \/ ScrubCache

IngesterBegin(self) == /\ pc[self] = "IngesterBegin"
                       /\ \/ /\ pc' = [pc EXCEPT ![self] = "ReceiveInput"]
                          \/ /\ TRUE
                             /\ pc' = [pc EXCEPT ![self] = "Done"]
                       /\ UNCHANGED << now, current_epoch, delete_epoch, 
                                       series_metadata, 
                                       series_referenced_from_data, 
                                       cached_series, observed_epochs, 
                                       fetched_epoch, fetched_series, 
                                       new_series, new_references, 
                                       locally_observed_epoch, candidates >>

ReceiveInput(self) == /\ pc[self] = "ReceiveInput"
                      /\ \E series \in SeriesId:
                           new_series' = [new_series EXCEPT ![self] = { series }]
                      /\ pc' = [pc EXCEPT ![self] = "CacheLookupTransaction"]
                      /\ UNCHANGED << now, current_epoch, delete_epoch, 
                                      series_metadata, 
                                      series_referenced_from_data, 
                                      cached_series, observed_epochs, 
                                      fetched_epoch, fetched_series, 
                                      new_references, locally_observed_epoch, 
                                      candidates >>

CacheLookupTransaction(self) == /\ pc[self] = "CacheLookupTransaction"
                                /\ new_references' = [new_references EXCEPT ![self] = new_series[self]]
                                /\ new_series' = [new_series EXCEPT ![self] = new_series[self] \ cached_series[self]]
                                /\ series_metadata' = [x \in new_series'[self] |-> NewEntry] @@ series_metadata
                                /\ cached_series' = [cached_series EXCEPT ![self] = cached_series[self] \union new_series'[self]]
                                /\ locally_observed_epoch' = [locally_observed_epoch EXCEPT ![self] = observed_epochs[self]]
                                /\ pc' = [pc EXCEPT ![self] = "IngestTransaction"]
                                /\ UNCHANGED << now, current_epoch, 
                                                delete_epoch, 
                                                series_referenced_from_data, 
                                                observed_epochs, fetched_epoch, 
                                                fetched_series, candidates >>

IngestTransaction(self) == /\ pc[self] = "IngestTransaction"
                           /\ IF locally_observed_epoch[self] <= delete_epoch
                                 THEN /\ TRUE
                                      /\ UNCHANGED series_referenced_from_data
                                 ELSE /\ series_referenced_from_data' = (series_referenced_from_data \union new_references[self])
                           /\ pc' = [pc EXCEPT ![self] = "IngesterBegin"]
                           /\ UNCHANGED << now, current_epoch, delete_epoch, 
                                           series_metadata, cached_series, 
                                           observed_epochs, fetched_epoch, 
                                           fetched_series, new_series, 
                                           new_references, 
                                           locally_observed_epoch, candidates >>

ingester(self) == IngesterBegin(self) \/ ReceiveInput(self)
                     \/ CacheLookupTransaction(self)
                     \/ IngestTransaction(self)

BgWorkerTxBegin(self) == /\ pc[self] = "BgWorkerTxBegin"
                         /\ IF now < MaxEpochs
                               THEN /\ \E dt \in 0..1:
                                         now' = now + dt
                                    /\ \/ /\ pc' = [pc EXCEPT ![self] = "DropChunkData"]
                                       \/ /\ pc' = [pc EXCEPT ![self] = "AdvanceEpoch"]
                                       \/ /\ pc' = [pc EXCEPT ![self] = "PrepareDeleteTx"]
                               ELSE /\ pc' = [pc EXCEPT ![self] = "Done"]
                                    /\ now' = now
                         /\ UNCHANGED << current_epoch, delete_epoch, 
                                         series_metadata, 
                                         series_referenced_from_data, 
                                         cached_series, observed_epochs, 
                                         fetched_epoch, fetched_series, 
                                         new_series, new_references, 
                                         locally_observed_epoch, candidates >>

DropChunkData(self) == /\ pc[self] = "DropChunkData"
                       /\ \E series_to_drop \in SeriesId:
                            series_referenced_from_data' = series_referenced_from_data \ {series_to_drop}
                       /\ pc' = [pc EXCEPT ![self] = "BgWorkerTxBegin"]
                       /\ UNCHANGED << now, current_epoch, delete_epoch, 
                                       series_metadata, cached_series, 
                                       observed_epochs, fetched_epoch, 
                                       fetched_series, new_series, 
                                       new_references, locally_observed_epoch, 
                                       candidates >>

AdvanceEpoch(self) == /\ pc[self] = "AdvanceEpoch"
                      /\ current_epoch' = Max({current_epoch, now})
                      /\ pc' = [pc EXCEPT ![self] = "MarkUnused"]
                      /\ UNCHANGED << now, delete_epoch, series_metadata, 
                                      series_referenced_from_data, 
                                      cached_series, observed_epochs, 
                                      fetched_epoch, fetched_series, 
                                      new_series, new_references, 
                                      locally_observed_epoch, candidates >>

MarkUnused(self) == /\ pc[self] = "MarkUnused"
                    /\ LET stale_series == {s \in StoredSeries: s \notin series_referenced_from_data} IN
                         series_metadata' = [s \in stale_series |-> EntryMarkedForDeletion(current_epoch)] @@ series_metadata
                    /\ pc' = [pc EXCEPT ![self] = "BgWorkerTxBegin"]
                    /\ UNCHANGED << now, current_epoch, delete_epoch, 
                                    series_referenced_from_data, cached_series, 
                                    observed_epochs, fetched_epoch, 
                                    fetched_series, new_series, new_references, 
                                    locally_observed_epoch, candidates >>

PrepareDeleteTx(self) == /\ pc[self] = "PrepareDeleteTx"
                         /\ delete_epoch' = Max({current_epoch - Delay, delete_epoch})
                         /\ pc' = [pc EXCEPT ![self] = "ActuallyDeleteTx"]
                         /\ UNCHANGED << now, current_epoch, series_metadata, 
                                         series_referenced_from_data, 
                                         cached_series, observed_epochs, 
                                         fetched_epoch, fetched_series, 
                                         new_series, new_references, 
                                         locally_observed_epoch, candidates >>

ActuallyDeleteTx(self) == /\ pc[self] = "ActuallyDeleteTx"
                          /\ candidates' = [candidates EXCEPT ![self] = MarkedAndRipe(delete_epoch) \ NewDataAfterMarked]
                          /\ series_metadata' = [x \in candidates'[self] |-> NoEntry] @@ series_metadata
                          /\ pc' = [pc EXCEPT ![self] = "Ressurect"]
                          /\ UNCHANGED << now, current_epoch, delete_epoch, 
                                          series_referenced_from_data, 
                                          cached_series, observed_epochs, 
                                          fetched_epoch, fetched_series, 
                                          new_series, new_references, 
                                          locally_observed_epoch >>

Ressurect(self) == /\ pc[self] = "Ressurect"
                   /\ series_metadata' = [x \in (MarkedAndRipe(delete_epoch) \ candidates[self]) |-> NewEntry] @@ series_metadata
                   /\ pc' = [pc EXCEPT ![self] = "BgWorkerTxBegin"]
                   /\ UNCHANGED << now, current_epoch, delete_epoch, 
                                   series_referenced_from_data, cached_series, 
                                   observed_epochs, fetched_epoch, 
                                   fetched_series, new_series, new_references, 
                                   locally_observed_epoch, candidates >>

bg_worker(self) == BgWorkerTxBegin(self) \/ DropChunkData(self)
                      \/ AdvanceEpoch(self) \/ MarkUnused(self)
                      \/ PrepareDeleteTx(self) \/ ActuallyDeleteTx(self)
                      \/ Ressurect(self)

(* Allow infinite stuttering to prevent deadlock on termination. *)
Terminating == /\ \A self \in ProcSet: pc[self] = "Done"
               /\ UNCHANGED vars

Next == cache_refresh_worker
           \/ (\E self \in Ingesters: ingester(self))
           \/ (\E self \in BgWorkers: bg_worker(self))
           \/ Terminating

Spec == Init /\ [][Next]_vars

Termination == <>(\A self \in ProcSet: pc[self] = "Done")

\* END TRANSLATION 

(* 
 * If a series appears as new then at least sometimes it is stored.
 *
 * This is a pretty weak liveness property, but this spec's main focus
 * is safety. This property exists mainly to guard against a spec
 * that doesn't insert any data and thus trivally satisifies safety.
 *)
NewSeriesAreStored == <>(
    \A i \in Ingesters: (
        \A s \in SeriesId: 
            s \in new_series[i] => series_metadata[s].stored = TRUE
    ))

====================================================================
