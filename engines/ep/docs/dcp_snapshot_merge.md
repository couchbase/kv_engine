# ActiveStream: checkpoints, snapshot markers and the replica backfill/memory merge

This document describes how an ActiveStream transforms checkpoints into DCP
snapshots, and then the special behaviour required when a DCP producer streams
from a replica vbucket - where a backfill from disk must be merged with the
in-memory checkpoint data (MB-73179 and related fixes).

## Background: checkpoints vs DCP snapshots

A vbucket's mutations are queued into checkpoints, managed by the
CheckpointManager. A DCP ActiveStream registers a cursor with the
CheckpointManager and repeatedly pulls batches of items from it
(`CheckpointManager::getItemsForCursor`), transforming each batch into DCP
messages in `ActiveStream::processItems`.

DCP has no explicit checkpoint start/end messages. Instead a sequence of
mutations is prefixed by a `DcpSnapshotMarker{start, end, flags}`. The marker
is a promise to the client:

* every seqno in `[start, end]` that the client is _entitled_ to will be sent
  and
* the snapshot's advertised `end` seqno will always be delivered - either as
  the item itself, or as a `DcpSeqnoAdvanced` standing in for it (e.g. when
  the item is filtered from the stream). This is the fundamental protocol
  rule referenced throughout this document.

### One checkpoint, many markers

An ActiveStream does not wait for a checkpoint to close before streaming it.
Each batch pulled by the cursor is sent under its own snapshot marker, so a
single checkpoint is commonly split into several consecutive markers:

```
Checkpoint [1,100]  (open, still being written to)

cursor batch 1: seqnos 1-20    ->  marker [1,20]   + mutations 1-20
cursor batch 2: seqnos 21-45   ->  marker [21,45]  + mutations 21-45
cursor batch 3: seqnos 46-100  ->  marker [46,100] + mutations 46-100
```

Splitting is correct because **deduplication is not permitted over a cursor**.
Within a checkpoint, a new mutation of a key may deduplicate (replace) an
older queued item for the same key - but only when no cursor still has that
older item ahead of it. Once the DCP cursor has passed an item (it has been
pulled into a batch), that item's range is immutable: nothing the client has
been promised can be retrospectively removed or replaced. Each emitted range
`[a,b]` is therefore a complete, self-consistent snapshot in its own right; a
newer version of a key queued after the cursor passed simply arrives in a
later snapshot, which the protocol permits.

## Streaming from a replica: why backfill and memory must merge

The DCP protocol allows for a replica to mirror the active vbucket's
checkpoints. The snapshot flag "Checkpoint" is used to create new checkpoints
and set the current snapshot range and snapshot markers without that extend that
range.

Thus when the active transmits one checkpoint using many markers, the initial
marker represents the start of that checkpoint and is tagged "Checkpoint" and
the non tagged markers which follow thus an extension of the current checkpoint.
The snapshot markers are still setting the consistent boundaries and when
producer is created upon a replica, those boundaries must still be respected
when that producer transmits snapshots. The merging is therefore a critical
part of maintaining those boundaries when a replica has to backfill from disk.

Persistence of a complete snapshot can be delayed (network transmission,
flusher delays), thus the disk of a replica can be in a partial snapshot state.
Consider a replica that is receiving snapshot [1,3]:

```
active sends snapshot [1,3]
replica receives seqno:1, seqno:2 ... and flushes them
disk:   seqnos 1-2, vbucket_state records "persisted snapshot [1,3]"
memory: checkpoint [1,3], awaiting seqno:3
```

A new stream backfills seqnos 1-2 from disk. The raw disk high seqno (2) is
*not* a consistent point - the client must not be told that a snapshot ends at
2 when the snapshot being persisted really ends at 3. `markDiskSnapshot`
therefore **merges** the backfill with the persisted snapshot: the disk
marker's end is extended to the persisted snapshot end.

```
wire: marker [0,3] (Disk|Checkpoint)   <- merged: disk end 2 extended to 3
      mutations 1,2                     <- from backfill
      ... seqno:3 arrives later from the in-memory phase, under the SAME
          marker - no new marker may be sent for it
```

## The merge window: mergeEndSeqno

When `markDiskSnapshot` merges, it records the merged end in
`ActiveStream::mergeEndSeqno`. Until the stream has streamed up to that seqno
the client is *inside* the merged snapshot, and `ActiveStream::snapshot` (the
in-memory phase) must respect that:

* no new snapshot marker may be generated for items within the merged range
  (the client already has the marker; additionally `lastSentSnapEndSeqno` is
  strictly monotonic and re-sending would break it - the original
  MB-71914 exception), and
* the merged marker's advertised end must still be delivered, per the
  protocol rule above.

A batch of items processed by `ActiveStream::snapshot` may end in one of
three positions relative to `mergeEndSeqno`:

### Case 1 - the batch ends before mergeEndSeqno

The items are pushed with no marker; the merge window stays in force for
subsequent batches. A merged snapshot may be consumed over any number of
batches.

### Case 2 - the batch ends on mergeEndSeqno

The items are pushed with no marker and the merge window is closed. If the
item at `mergeEndSeqno` itself was filtered from the stream (e.g. a mutation
in a collection the stream doesn't include), no mutation delivers the
advertised end - a `DcpSeqnoAdvanced` is queued in its place.

Note a subtlety which shapes the implementation: the item at `mergeEndSeqno`
can be removed from the stream in two different ways, and they advance
different seqno counters. A *collection-filtered* mutation still advances the
stream's `lastReadSeqno`; a *non-visible* item (a prepare/abort which cannot
be sent on a non-sync-write stream) advances only `curChkSeqno`. In either
case the generic "is the snapshot complete" checks
(`isReplicaSnapshotComplete`, `isSeqnoGapAtEndOfSnapshot`) cannot distinguish
"complete" from "complete but end never delivered" - hence the explicit
merge-window tracking, evaluated outside the marker-generation block and
keyed on `curChkSeqno` (the cursor's consumed-up-to seqno, which both cases
advance). Closing the window exactly once matters: were it left set after the
snapshot was closed, the next batch would re-close it via
`completeMergedSnapshot` and send a duplicate `DcpSeqnoAdvanced` (test:
`PrepareAtMergeEndNoDuplicateSeqnoAdvance`).

### Case 3 - the batch passes mergeEndSeqno

`completeMergedSnapshot` splits the batch at the merge boundary:

1. leading items with seqnos `<= mergeEndSeqno` are pushed first - they belong
   to the merged marker (test: `SplitAtMergeBoundary`);
2. if the item at `mergeEndSeqno` was filtered, a `DcpSeqnoAdvanced` to
   `mergeEndSeqno` is queued to close the merged snapshot *before* any new
   marker (test: `SeqnoAdvanceClosesMergedSnapshotBeforeNewMarker`);
3. a new marker is then generated for the remaining items, starting at the
   first remaining seqno.

The batch may drain completely in step 1. That can only happen when the
snapshot end was raised to `highNonVisibleSeqno` - all deliverable items were
within the merged range and only a non-visible seqno (a prepare/abort which
cannot be sent on a non-sync-write stream) lies beyond it. The new marker then
advertises the item-less range `[highNonVisibleSeqno, highNonVisibleSeqno]`,
which is valid because a `DcpSeqnoAdvanced` delivering the marker's end always
follows. The snap-start is deliberately `snapEnd` rather than
`mergeEndSeqno+1`, which may never have been assigned to any item, visible or
not (test: `PrepareBeyondMergeEndDrainsBatch`).

## Worked example

The scenario used by most of the `MergeSnapshotTest` tests. The stream filters
on the default collection; `fruit` items are filtered out.

```
1. replica receives snapshot [1,3]
2. seqno:1 create-collection "fruit"          (persisted, then expelled)
3. seqno:2 default-collection mutation        (persisted)
4. stream created; backfill runs
     -> marker [0,3]  (merged: disk end 2 -> persisted snapshot end 3)
     -> mutation seqno:2  (seqno:1 filtered)
     mergeEndSeqno = 3
5. seqno:3 default mutation, seqno:4 fruit mutation arrive in memory
6. in-memory phase processes [3,4]:
     seqno:3 passes the filter, seqno:4 does not (but bumps lastReadSeqno)
     batch snapEnd (3) == mergeEndSeqno -> case 2
     -> mutation seqno:3   (no marker: completes the merged [0,3] snapshot)
7. any later item (e.g. seqno:5) starts a fresh marker as normal
```
