# DCP Design Discussion

This document covers additional details on why DCP works in certain ways, and gives examples of how it can break if certain rules are not followed.

## Why does a client need to provide snapshot start & end seqnos to resume a stream?

Due to [Deduplication](concepts.md#deduplication), the server needs to know the extent of the snapshot which the consumer was last sent to ensure the consumer doesn't miss any mutations on resumption.

Without snapshot start and end seqnos, the server doesn't know if the consumer is at a consistent point or not, nor what the last consistent point for that client was.

**Example 1** - the server needs to know which "timeline" the consumer' mutations came from, so in the event of the consumer resuming after a server restart, the server can correctly determine if the consumer needs to rollback or not.

Assuming the state of the vbucket's checkpoint manger is:

```
Seqno    [1]     2       3       4
Key      SET(A)  SET(B)  SET(C)  DEL(A)
```
_`[N]` = mutation has been deduplicated_

, consider the following sequence of events:

1. Consumer establishes a stream and gets vb UUID=`AAAA`.
2. Consumer receives a Snapshot `start=1`, `end=4`.
3. Consumer receives 2 mutations - `2:SET(B)` and `3:SET(C)`
4. Consumer disconnects, server restarts and generates a new vb UUID=`BBBB` at seqno 3.
5. Consumer reconnects, _incorrectly_ sets `start=3`, `snapshot_start=3`, `snapshot_end=3`.
6. Server will perform rollback check and **incorrectly conclude that the consumer doesn't need to rollback** - as the vBucket branched _after_ the snapshot the consumer received.

This could result in lost DCP messages. The server would have sent the following messages to the consumer:

* `SnapshotMarker(start=1, end=4)`
* `2:SET(B)`
* `3:SET(C)`
* `4:DEL(A)`

However, the consumer only received the Snapshot Marker, `2:SET(B)` and `3:SET(C)`. After the consumer disconnects / server restarts, the checkpoint manager has lost `4:DEL(A)` (as it was never persisted to disk) and a vb UUID branch point is created at the last common point:

```
Seqno    [1]     2       3       4
Key      SET(A)  SET(B)  SET(C)  <<new mutations...>
                         |
                         UUID:BBBB
```

i.e. `1:SET(A)` is no longer deduplicated, as `4:DEL(A)` was lost in the restart. However, as the consumer has incorrectly told the server it had a complete snapshot from _before_ the UUID changed, then the server thinks it can send from seqno 3 onwards. *This would result in document A never being sent to the DCP consumer*.

This can be demonstrated using `humpty-dumpty` to test different failover scenarios. First, the _correct_ resume request for this scenario:

```
$ echo "1111 1 4 3" | ./humpty_dumpty failover.json 10 0
Simulating behaviour of VBucket with highSeqno: 10, purgeSeqno:0, failoverTable:
[
    {"id":2222,"seq":3}
    {"id":1111,"seq":0}
]

Testing UUID:1111 snapshot:{1,4} start:3
  Rollback:true
  Requested rollback seqno:1
  Reason: consumer ahead of producer - producer upper at 3
```

Incorrectly specifying snapshot start and end as 3 does not rollback as it should:

````
$ echo "1111 3 3 3" | ./humpty_dumpty failover.json 10 0
Simulating behaviour of VBucket with highSeqno: 10, purgeSeqno:0, failoverTable:
[
    {"id":2222,"seq":3}
    {"id":1111,"seq":0}
]

Testing UUID:1111 snapshot:{3,3} start:3
  Rollback:false

```
