---- MODULE cache ----
EXTENDS TLC, Integers, Sequences, FiniteSets

CONSTANT 
    NULL,
    MaxSeries,
    Ingesters,
    BgWorkers
ASSUME 
    /\ MaxSeries \in Nat 
    /\ Cardinality(BgWorkers) = 1
    /\ BgWorkers \intersect Ingesters = {}

SeriesId == 1..MaxSeries
Epoch == Nat
SeriesEntry ==
    [
        marked_at: Epoch \union {NULL},
        stored: BOOLEAN \* In TLA function sets are total, therefore we need an explicit flag
    ]
NoEntry == [marked_at |-> NULL, stored |-> FALSE]
NewEntry == [marked_at |-> NULL, stored |-> TRUE]
EntryMarkedForDeletion(e) == [marked_at |-> e, stored |-> TRUE]

(*--algorithm cache
variables
    series_metadata = [x \in SeriesId |-> NoEntry];
    series_referenced_from_data = {};
    cached_series = [i \in Ingesters |-> {}];

define
    TypeInvariant == 
        /\ series_metadata \in [SeriesId -> SeriesEntry]
        /\ series_referenced_from_data \in SUBSET SeriesId
        /\ cached_series \in [Ingesters -> SUBSET SeriesId]

    ForeignKeySafety ==
        /\ \A sid \in series_referenced_from_data: series_metadata[sid].stored = TRUE

    MarkedSeries == {s \in DOMAIN series_metadata: series_metadata[s].marked_at # NULL}

end define;

process ingester \in Ingesters
variables 
    new_series = {};
    new_references = {};
begin
    ReceiveInput:
        either
            with series \in SUBSET SeriesId \ {} do 
                new_series := series;
            end with;
            CacheLookup:
                new_references := new_series;
                new_series := new_series \ cached_series[self];
            \* CacheUpdate:
            \*     cached_series[self] := cached_series[self] \union new_series;
            IngestTransaction:
                series_metadata := [x \in new_series |-> NewEntry] @@ series_metadata;
                series_referenced_from_data := series_referenced_from_data \union new_references;
                goto ReceiveInput;
        or
            \* Stopped receiving data
            skip;
        end either;
end process;        

process bg_worker \in BgWorkers
begin
    BgWorkerTxBegin:
        (* We assume background workers mutually exclude by grabbing a lock on the epoch table *)
        either
            MarkWorker:
                with stale_series \in SUBSET SeriesId \ {} do 
                    series_metadata := 
                        [x \in stale_series |-> EntryMarkedForDeletion(1)] @@ series_metadata;
                    goto BgWorkerTxBegin;
                end with;
        or
            SweepWorker:
                series_metadata := [x \in MarkedSeries |-> NoEntry] @@ series_metadata;
                series_referenced_from_data := series_referenced_from_data \ MarkedSeries;
                goto BgWorkerTxBegin;
        or
            skip;
        end either;
end process;        
end algorithm; *)
    
\* BEGIN TRANSLATION (chksum(pcal) = "1cfda9f3" /\ chksum(tla) = "c5e79bec")
VARIABLES series_metadata, series_referenced_from_data, cached_series, pc

(* define statement *)
TypeInvariant ==
    /\ series_metadata \in [SeriesId -> SeriesEntry]
    /\ series_referenced_from_data \in SUBSET SeriesId
    /\ cached_series \in [Ingesters -> SUBSET SeriesId]

ForeignKeySafety ==
    /\ \A sid \in series_referenced_from_data: series_metadata[sid].stored = TRUE

MarkedSeries == {s \in DOMAIN series_metadata: series_metadata[s].marked_at # NULL}

VARIABLES new_series, new_references

vars == << series_metadata, series_referenced_from_data, cached_series, pc, 
           new_series, new_references >>

ProcSet == (Ingesters) \cup (BgWorkers)

Init == (* Global variables *)
        /\ series_metadata = [x \in SeriesId |-> NoEntry]
        /\ series_referenced_from_data = {}
        /\ cached_series = [i \in Ingesters |-> {}]
        (* Process ingester *)
        /\ new_series = [self \in Ingesters |-> {}]
        /\ new_references = [self \in Ingesters |-> {}]
        /\ pc = [self \in ProcSet |-> CASE self \in Ingesters -> "ReceiveInput"
                                        [] self \in BgWorkers -> "BgWorkerTxBegin"]

ReceiveInput(self) == /\ pc[self] = "ReceiveInput"
                      /\ \/ /\ \E series \in SUBSET SeriesId \ {}:
                                 new_series' = [new_series EXCEPT ![self] = series]
                            /\ pc' = [pc EXCEPT ![self] = "CacheLookup"]
                         \/ /\ TRUE
                            /\ pc' = [pc EXCEPT ![self] = "Done"]
                            /\ UNCHANGED new_series
                      /\ UNCHANGED << series_metadata, 
                                      series_referenced_from_data, 
                                      cached_series, new_references >>

CacheLookup(self) == /\ pc[self] = "CacheLookup"
                     /\ new_references' = [new_references EXCEPT ![self] = new_series[self]]
                     /\ new_series' = [new_series EXCEPT ![self] = new_series[self] \ cached_series[self]]
                     /\ pc' = [pc EXCEPT ![self] = "IngestTransaction"]
                     /\ UNCHANGED << series_metadata, 
                                     series_referenced_from_data, 
                                     cached_series >>

IngestTransaction(self) == /\ pc[self] = "IngestTransaction"
                           /\ series_metadata' = [x \in new_series[self] |-> NewEntry] @@ series_metadata
                           /\ series_referenced_from_data' = (series_referenced_from_data \union new_references[self])
                           /\ pc' = [pc EXCEPT ![self] = "ReceiveInput"]
                           /\ UNCHANGED << cached_series, new_series, 
                                           new_references >>

ingester(self) == ReceiveInput(self) \/ CacheLookup(self)
                     \/ IngestTransaction(self)

BgWorkerTxBegin(self) == /\ pc[self] = "BgWorkerTxBegin"
                         /\ \/ /\ pc' = [pc EXCEPT ![self] = "MarkWorker"]
                            \/ /\ pc' = [pc EXCEPT ![self] = "SweepWorker"]
                            \/ /\ TRUE
                               /\ pc' = [pc EXCEPT ![self] = "Done"]
                         /\ UNCHANGED << series_metadata, 
                                         series_referenced_from_data, 
                                         cached_series, new_series, 
                                         new_references >>

MarkWorker(self) == /\ pc[self] = "MarkWorker"
                    /\ \E stale_series \in SUBSET SeriesId \ {}:
                         /\ series_metadata' = [x \in stale_series |-> EntryMarkedForDeletion(1)] @@ series_metadata
                         /\ pc' = [pc EXCEPT ![self] = "BgWorkerTxBegin"]
                    /\ UNCHANGED << series_referenced_from_data, cached_series, 
                                    new_series, new_references >>

SweepWorker(self) == /\ pc[self] = "SweepWorker"
                     /\ series_metadata' = [x \in MarkedSeries |-> NoEntry] @@ series_metadata
                     /\ series_referenced_from_data' = series_referenced_from_data \ MarkedSeries
                     /\ pc' = [pc EXCEPT ![self] = "BgWorkerTxBegin"]
                     /\ UNCHANGED << cached_series, new_series, new_references >>

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
