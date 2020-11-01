# Epoch-Based cache validation #

Previously our SeriesID cache would never delete entries. This could
cause issues as SeriesIDs are deleted from the database if we see that
no data table uses them; in the event that the series is resurrected
after that, new rows in that series would contain an ID that did not
refer to any series set stored.

In order to fix this, we introduce an epoch mechanism to validate that
the SeriesIDs and INSERT is adding to the DB could not have been
deleted. At a high level, the mechanism works as follows:
  - The database stores an epoch counter that monotonically increases
    whenever SeriesIDs are deleted.
  - The SeriesID cache stores the smallest epoch at which it read an
    ID from the DB. This epoch is sent to the inserter along with the
    data to be inserted.
  - As the first statement of the INSERT transaction, the inserter
    validates that the epoch at the DB is low enough that none of the
    SeriesIDs being inserted could have been deleted. If the epoch
    isn't valid the INSERT transaction is aborted.

Choosing which epochs are valid for insertions has subtle performance
implications. While we could dictate that the epoch we read must match
the epoch we see during an INSERT exactly, this would cause thundering
herd aborts whenever we delete SeriesIDs. Instead, we allow the epoch
at insert to be 1 greater than the one we saw at read time, and delay
the deletion of SeriesIDs until after they've been unused for at least
a full epoch. Graphically, with R being the earliest point in the
epoch numberline at which we read, and I being the point we write, we
allow read/insert combinations like:
```
  R              R
0|1|2|3   or   0|1|2|3
  I                I
```

but not ones like
```
  R              R
0|1|2|3   or   0|1|2|3
      I        I
```

Similarly, if M is the first time we mark a SeriesID as being deletable
because there are no data columns that reference it, and D is the
point at which we actually delete it (note that it is these deletes
that increment the epoch, so they properly happen /between/ epochs. We
allow mark/delete combinations like:
```
M
0|1|2|3
     D
```

but not ones like
```
M                M              M
0|1|2|3   or   0|1|2|3   or   0|1|2|3
 D                  D          D
```

Over all the valid read/insert and mark/delete combinations nest
something like one of:
```
M R            M R
0|1|2|3   or   0|1|2|3
  I  D             ID
```

So a recheck to see if SeriesIDs are still live before actual deletion
ensures that no dead IDs are inserted.
