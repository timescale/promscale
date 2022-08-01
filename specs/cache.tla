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

define
    TypeInvariant == 
        /\ now \in Epoch
        /\ current_epoch \in Epoch
        /\ delete_epoch \in Epoch
        /\ series_metadata \in [SeriesId -> SeriesEntry]
        /\ series_referenced_from_data \in SUBSET SeriesId
        /\ cached_series \in [Ingesters -> SUBSET SeriesId]

    ForeignKeySafety ==
        /\ \A sid \in series_referenced_from_data: series_metadata[sid].stored = TRUE

    StoredSeries == 
        {s \in DOMAIN series_metadata: series_metadata[s].stored = TRUE}

    MarkedSeries == 
        {s \in DOMAIN series_metadata: series_metadata[s].marked_at # NULL}

    MarkedAndRipe == 
        {s \in MarkedSeries: (series_metadata[s].marked_at) + Delay < current_epoch }

end define;

process ingester \in Ingesters
variables 
    new_series = {};
    new_references = {};
    observed_epoch = 0;
begin
    IngesterBegin:
        either
            ReceiveInput:
                (*
                * Technically multiple series could be ingested at once.
                * This spec should achieve an equivalent behaviour by
                * passing through this label multiple times in a row.
                *)
                with series \in SeriesId do 
                    new_series := { series };
                end with;
            CacheLookup:
                new_references := new_series;
                new_series := new_series \ cached_series[self];
                cached_series[self] := cached_series[self] \union new_series;
            IngestTransaction:
                \* this if is epoch_abort and should be extracted and named
                if observed_epoch <= delete_epoch then 
                    \* TODO extract cache invalidation into its own process
                    (* 
                     * Should use SELECT FOR SHARE when fetching current_epoch,
                     * To ensure a concurrent Mark worker didn't make changes.
                     *)
                    observed_epoch := current_epoch;
                    (* SELECT id FROM _prom_catalog.series WHERE delete_at IS NOT NULL *)
                    cached_series[self] := StoredSeries \ MarkedSeries;
                else
                    (* 
                     * get_or_create_series_id calls ressurect_series_id 
                     * which, in turn, uses UPDATE and will cause a transaction
                     * conflict with a simultaneous deletion attempt.
                     *)
                    series_metadata := [x \in new_series |-> NewEntry] @@ series_metadata;
                    series_referenced_from_data := series_referenced_from_data \union new_references;
                end if;
                goto IngesterBegin;
        or
            (* Ingester is done *)
            skip;
        end either;
end process;        

process bg_worker \in BgWorkers
begin
    BgWorkerTxBegin:
        while now < MaxEpochs do
            (* When a backgroud worker starts a transaction, sometimes some time has passed. *)
            with dt \in 0..1 do
                now := now + dt;
            end with;
            (* We assume background workers mutually exclude by grabbing a lock on the epoch table *)
            either
                MarkWorker:
                    current_epoch := Max({current_epoch, now});
                    (*
                    * Technically multiple series could be marked at once.
                    * This spec is supposed to achieve this by entering 
                    * this branch of either without advancing now.
                    *)
                    with stale_series \in SeriesId do 
                        series_metadata := 
                            (stale_series :> EntryMarkedForDeletion(current_epoch)) @@ series_metadata;
                    end with;
            or
                SweepWorker:
                    if MarkedAndRipe # {} then
                        delete_epoch := Max({series_metadata[s].marked_at: s \in MarkedAndRipe});
                        (* 
                        * DELETE should provoke a conflict with
                        * get_or_create_series_id called by Ingester
                        *)
                        series_metadata := [x \in MarkedAndRipe |-> NoEntry] @@ series_metadata;
                        series_referenced_from_data := series_referenced_from_data \ MarkedAndRipe;
                    end if;
            end either;
        end while;
end process;        
end algorithm; *)
    
\* BEGIN TRANSLATION (chksum(pcal) = "701886c2" /\ chksum(tla) = "cda8acd0")
VARIABLES now, current_epoch, delete_epoch, series_metadata, 
          series_referenced_from_data, cached_series, pc

(* define statement *)
TypeInvariant ==
    /\ now \in Epoch
    /\ current_epoch \in Epoch
    /\ delete_epoch \in Epoch
    /\ series_metadata \in [SeriesId -> SeriesEntry]
    /\ series_referenced_from_data \in SUBSET SeriesId
    /\ cached_series \in [Ingesters -> SUBSET SeriesId]

ForeignKeySafety ==
    /\ \A sid \in series_referenced_from_data: series_metadata[sid].stored = TRUE

StoredSeries ==
    {s \in DOMAIN series_metadata: series_metadata[s].stored = TRUE}

MarkedSeries ==
    {s \in DOMAIN series_metadata: series_metadata[s].marked_at # NULL}

MarkedAndRipe ==
    {s \in MarkedSeries: (series_metadata[s].marked_at) + Delay < current_epoch }

VARIABLES new_series, new_references, observed_epoch

vars == << now, current_epoch, delete_epoch, series_metadata, 
           series_referenced_from_data, cached_series, pc, new_series, 
           new_references, observed_epoch >>

ProcSet == (Ingesters) \cup (BgWorkers)

Init == (* Global variables *)
        /\ now = 0
        /\ current_epoch = 0
        /\ delete_epoch = 0
        /\ series_metadata = [x \in SeriesId |-> NoEntry]
        /\ series_referenced_from_data = {}
        /\ cached_series = [i \in Ingesters |-> {}]
        (* Process ingester *)
        /\ new_series = [self \in Ingesters |-> {}]
        /\ new_references = [self \in Ingesters |-> {}]
        /\ observed_epoch = [self \in Ingesters |-> 0]
        /\ pc = [self \in ProcSet |-> CASE self \in Ingesters -> "IngesterBegin"
                                        [] self \in BgWorkers -> "BgWorkerTxBegin"]

IngesterBegin(self) == /\ pc[self] = "IngesterBegin"
                       /\ \/ /\ pc' = [pc EXCEPT ![self] = "ReceiveInput"]
                          \/ /\ TRUE
                             /\ pc' = [pc EXCEPT ![self] = "Done"]
                       /\ UNCHANGED << now, current_epoch, delete_epoch, 
                                       series_metadata, 
                                       series_referenced_from_data, 
                                       cached_series, new_series, 
                                       new_references, observed_epoch >>

ReceiveInput(self) == /\ pc[self] = "ReceiveInput"
                      /\ \E series \in SeriesId:
                           new_series' = [new_series EXCEPT ![self] = { series }]
                      /\ pc' = [pc EXCEPT ![self] = "CacheLookup"]
                      /\ UNCHANGED << now, current_epoch, delete_epoch, 
                                      series_metadata, 
                                      series_referenced_from_data, 
                                      cached_series, new_references, 
                                      observed_epoch >>

CacheLookup(self) == /\ pc[self] = "CacheLookup"
                     /\ new_references' = [new_references EXCEPT ![self] = new_series[self]]
                     /\ new_series' = [new_series EXCEPT ![self] = new_series[self] \ cached_series[self]]
                     /\ cached_series' = [cached_series EXCEPT ![self] = cached_series[self] \union new_series'[self]]
                     /\ pc' = [pc EXCEPT ![self] = "IngestTransaction"]
                     /\ UNCHANGED << now, current_epoch, delete_epoch, 
                                     series_metadata, 
                                     series_referenced_from_data, 
                                     observed_epoch >>

IngestTransaction(self) == /\ pc[self] = "IngestTransaction"
                           /\ IF observed_epoch[self] <= delete_epoch
                                 THEN /\ observed_epoch' = [observed_epoch EXCEPT ![self] = current_epoch]
                                      /\ cached_series' = [cached_series EXCEPT ![self] = StoredSeries \ MarkedSeries]
                                      /\ UNCHANGED << series_metadata, 
                                                      series_referenced_from_data >>
                                 ELSE /\ series_metadata' = [x \in new_series[self] |-> NewEntry] @@ series_metadata
                                      /\ series_referenced_from_data' = (series_referenced_from_data \union new_references[self])
                                      /\ UNCHANGED << cached_series, 
                                                      observed_epoch >>
                           /\ pc' = [pc EXCEPT ![self] = "IngesterBegin"]
                           /\ UNCHANGED << now, current_epoch, delete_epoch, 
                                           new_series, new_references >>

ingester(self) == IngesterBegin(self) \/ ReceiveInput(self)
                     \/ CacheLookup(self) \/ IngestTransaction(self)

BgWorkerTxBegin(self) == /\ pc[self] = "BgWorkerTxBegin"
                         /\ IF now < MaxEpochs
                               THEN /\ \E dt \in 0..1:
                                         now' = now + dt
                                    /\ \/ /\ pc' = [pc EXCEPT ![self] = "MarkWorker"]
                                       \/ /\ pc' = [pc EXCEPT ![self] = "SweepWorker"]
                               ELSE /\ pc' = [pc EXCEPT ![self] = "Done"]
                                    /\ now' = now
                         /\ UNCHANGED << current_epoch, delete_epoch, 
                                         series_metadata, 
                                         series_referenced_from_data, 
                                         cached_series, new_series, 
                                         new_references, observed_epoch >>

MarkWorker(self) == /\ pc[self] = "MarkWorker"
                    /\ current_epoch' = Max({current_epoch, now})
                    /\ \E stale_series \in SeriesId:
                         series_metadata' = (stale_series :> EntryMarkedForDeletion(current_epoch')) @@ series_metadata
                    /\ pc' = [pc EXCEPT ![self] = "BgWorkerTxBegin"]
                    /\ UNCHANGED << now, delete_epoch, 
                                    series_referenced_from_data, cached_series, 
                                    new_series, new_references, observed_epoch >>

SweepWorker(self) == /\ pc[self] = "SweepWorker"
                     /\ IF MarkedAndRipe # {}
                           THEN /\ delete_epoch' = Max({series_metadata[s].marked_at: s \in MarkedAndRipe})
                                /\ series_metadata' = [x \in MarkedAndRipe |-> NoEntry] @@ series_metadata
                                /\ series_referenced_from_data' = series_referenced_from_data \ MarkedAndRipe
                           ELSE /\ TRUE
                                /\ UNCHANGED << delete_epoch, series_metadata, 
                                                series_referenced_from_data >>
                     /\ pc' = [pc EXCEPT ![self] = "BgWorkerTxBegin"]
                     /\ UNCHANGED << now, current_epoch, cached_series, 
                                     new_series, new_references, 
                                     observed_epoch >>

bg_worker(self) == BgWorkerTxBegin(self) \/ MarkWorker(self)
                      \/ SweepWorker(self)

(* Allow infinite stuttering to prevent deadlock on termination. *)
Terminating == /\ \A self \in ProcSet: pc[self] = "Done"
               /\ UNCHANGED vars

Next == (\E self \in Ingesters: ingester(self))
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
