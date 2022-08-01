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
    current_epoch = 0;
    delete_epoch = 0;
    series_metadata = [x \in SeriesId |-> NoEntry];
    series_referenced_from_data = {};
    cached_series = [i \in Ingesters |-> {}];

define
    TypeInvariant == 
        /\ current_epoch \in Epoch
        /\ delete_epoch \in Epoch
        /\ series_metadata \in [SeriesId -> SeriesEntry]
        /\ series_referenced_from_data \in SUBSET SeriesId
        /\ cached_series \in [Ingesters -> SUBSET SeriesId]

    ForeignKeySafety ==
        /\ \A sid \in series_referenced_from_data: series_metadata[sid].stored = TRUE

    MarkedSeries == 
        {s \in DOMAIN series_metadata: series_metadata[s].marked_at # NULL}

    MarkedAndRipe == 
        {s \in MarkedSeries: (series_metadata[s].marked_at) + Delay < current_epoch }

    (*
     * Here we substitute a value, obtained by running DELETE ... RETURNING max(marked_at)
     * and compute a semantically equivalent one to avoid the state space explosion.
     * 
     * The real delete_epoch is a maximum of marked_at of all deleted series,
     * a minimum of marked_at of surviving series is an upper bound.
     *)
    DeleteEpoch == 
        IF MarkedSeries # {} THEN 
            Min({series_metadata[s].marked_at: s \in MarkedSeries})
        ELSE 
            Max({current_epoch - 1, 0})

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
                with series \in SUBSET SeriesId \ {} do 
                    new_series := series;
                end with;
            CacheLookup:
                new_references := new_series;
                new_series := new_series \ cached_series[self];
            CacheUpdate:
                cached_series[self] := cached_series[self] \union new_series;
            IngestTransaction:
                \* this if is epoch_abort and should be extracted and named
                if observed_epoch <= DeleteEpoch then 
                    \* TODO extract cache invalidation into its own process
                    cached_series[self] := {};
                    (* Should use SELECT FOR SHARE when fetching current_epoch *)
                    observed_epoch := current_epoch;
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
            \* Stopped receiving data
            skip;
        end either;
end process;        

process bg_worker \in BgWorkers
begin
    BgWorkerTxBegin:
        if current_epoch < MaxEpochs then
            (* We assume background workers mutually exclude by grabbing a lock on the epoch table *)
            either
                MarkWorker:
                    current_epoch := current_epoch + 1;
                    with stale_series \in SUBSET SeriesId \ {} do 
                        series_metadata := 
                            [x \in stale_series |-> EntryMarkedForDeletion(current_epoch)] @@ series_metadata;
                        goto BgWorkerTxBegin;
                    end with;
            or
                SweepWorker:
                    if MarkedAndRipe # {} then
                        (* delete_epoch := Max({series_metadata[s].marked_at: s \in MarkedAndRipe}) *)
                        (* 
                        * DELETE should provoke a conflict with
                        * get_or_create_series_id calls ressurect_series_id 
                        * called by Ingester
                        *)
                        series_metadata := [x \in MarkedAndRipe |-> NoEntry] @@ series_metadata;
                        series_referenced_from_data := series_referenced_from_data \ MarkedAndRipe;
                    end if;
                    goto BgWorkerTxBegin;
            or
                skip;
            end either;
        end if;
end process;        
end algorithm; *)
    
\* BEGIN TRANSLATION (chksum(pcal) = "b5206a0e" /\ chksum(tla) = "5ea97ad8")
VARIABLES current_epoch, delete_epoch, series_metadata, 
          series_referenced_from_data, cached_series, pc

(* define statement *)
TypeInvariant ==
    /\ current_epoch \in Epoch
    /\ delete_epoch \in Epoch
    /\ series_metadata \in [SeriesId -> SeriesEntry]
    /\ series_referenced_from_data \in SUBSET SeriesId
    /\ cached_series \in [Ingesters -> SUBSET SeriesId]

ForeignKeySafety ==
    /\ \A sid \in series_referenced_from_data: series_metadata[sid].stored = TRUE

MarkedSeries ==
    {s \in DOMAIN series_metadata: series_metadata[s].marked_at # NULL}

MarkedAndRipe ==
    {s \in MarkedSeries: (series_metadata[s].marked_at) + Delay < current_epoch }








DeleteEpoch ==
    IF MarkedSeries # {} THEN
        Min({series_metadata[s].marked_at: s \in MarkedSeries})
    ELSE
        Max({current_epoch - 1, 0})

VARIABLES new_series, new_references, observed_epoch

vars == << current_epoch, delete_epoch, series_metadata, 
           series_referenced_from_data, cached_series, pc, new_series, 
           new_references, observed_epoch >>

ProcSet == (Ingesters) \cup (BgWorkers)

Init == (* Global variables *)
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
                       /\ UNCHANGED << current_epoch, delete_epoch, 
                                       series_metadata, 
                                       series_referenced_from_data, 
                                       cached_series, new_series, 
                                       new_references, observed_epoch >>

ReceiveInput(self) == /\ pc[self] = "ReceiveInput"
                      /\ \E series \in SUBSET SeriesId \ {}:
                           new_series' = [new_series EXCEPT ![self] = series]
                      /\ pc' = [pc EXCEPT ![self] = "CacheLookup"]
                      /\ UNCHANGED << current_epoch, delete_epoch, 
                                      series_metadata, 
                                      series_referenced_from_data, 
                                      cached_series, new_references, 
                                      observed_epoch >>

CacheLookup(self) == /\ pc[self] = "CacheLookup"
                     /\ new_references' = [new_references EXCEPT ![self] = new_series[self]]
                     /\ new_series' = [new_series EXCEPT ![self] = new_series[self] \ cached_series[self]]
                     /\ pc' = [pc EXCEPT ![self] = "CacheUpdate"]
                     /\ UNCHANGED << current_epoch, delete_epoch, 
                                     series_metadata, 
                                     series_referenced_from_data, 
                                     cached_series, observed_epoch >>

CacheUpdate(self) == /\ pc[self] = "CacheUpdate"
                     /\ cached_series' = [cached_series EXCEPT ![self] = cached_series[self] \union new_series[self]]
                     /\ pc' = [pc EXCEPT ![self] = "IngestTransaction"]
                     /\ UNCHANGED << current_epoch, delete_epoch, 
                                     series_metadata, 
                                     series_referenced_from_data, new_series, 
                                     new_references, observed_epoch >>

IngestTransaction(self) == /\ pc[self] = "IngestTransaction"
                           /\ IF observed_epoch[self] <= DeleteEpoch
                                 THEN /\ cached_series' = [cached_series EXCEPT ![self] = {}]
                                      /\ observed_epoch' = [observed_epoch EXCEPT ![self] = current_epoch]
                                      /\ UNCHANGED << series_metadata, 
                                                      series_referenced_from_data >>
                                 ELSE /\ series_metadata' = [x \in new_series[self] |-> NewEntry] @@ series_metadata
                                      /\ series_referenced_from_data' = (series_referenced_from_data \union new_references[self])
                                      /\ UNCHANGED << cached_series, 
                                                      observed_epoch >>
                           /\ pc' = [pc EXCEPT ![self] = "IngesterBegin"]
                           /\ UNCHANGED << current_epoch, delete_epoch, 
                                           new_series, new_references >>

ingester(self) == IngesterBegin(self) \/ ReceiveInput(self)
                     \/ CacheLookup(self) \/ CacheUpdate(self)
                     \/ IngestTransaction(self)

BgWorkerTxBegin(self) == /\ pc[self] = "BgWorkerTxBegin"
                         /\ IF current_epoch < MaxEpochs
                               THEN /\ \/ /\ pc' = [pc EXCEPT ![self] = "MarkWorker"]
                                       \/ /\ pc' = [pc EXCEPT ![self] = "SweepWorker"]
                                       \/ /\ TRUE
                                          /\ pc' = [pc EXCEPT ![self] = "Done"]
                               ELSE /\ pc' = [pc EXCEPT ![self] = "Done"]
                         /\ UNCHANGED << current_epoch, delete_epoch, 
                                         series_metadata, 
                                         series_referenced_from_data, 
                                         cached_series, new_series, 
                                         new_references, observed_epoch >>

MarkWorker(self) == /\ pc[self] = "MarkWorker"
                    /\ current_epoch' = current_epoch + 1
                    /\ \E stale_series \in SUBSET SeriesId \ {}:
                         /\ series_metadata' = [x \in stale_series |-> EntryMarkedForDeletion(current_epoch')] @@ series_metadata
                         /\ pc' = [pc EXCEPT ![self] = "BgWorkerTxBegin"]
                    /\ UNCHANGED << delete_epoch, series_referenced_from_data, 
                                    cached_series, new_series, new_references, 
                                    observed_epoch >>

SweepWorker(self) == /\ pc[self] = "SweepWorker"
                     /\ IF MarkedAndRipe # {}
                           THEN /\ series_metadata' = [x \in MarkedAndRipe |-> NoEntry] @@ series_metadata
                                /\ series_referenced_from_data' = series_referenced_from_data \ MarkedAndRipe
                           ELSE /\ TRUE
                                /\ UNCHANGED << series_metadata, 
                                                series_referenced_from_data >>
                     /\ pc' = [pc EXCEPT ![self] = "BgWorkerTxBegin"]
                     /\ UNCHANGED << current_epoch, delete_epoch, 
                                     cached_series, new_series, new_references, 
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

(* If a series appears as new then at least sometimes it is stored *)
NewSeriesAreStored == <>(
    \A i \in Ingesters: (
        \A s \in SeriesId: 
            s \in new_series[i] => series_metadata[s].stored = TRUE
    ))

====
