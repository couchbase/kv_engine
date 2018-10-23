# Mutation Queues

This document presents a design for mutation queues in EP Engine and UPR.  It addresses the spec (https://github.com/couchbaselabs/cbupr/blob/master/spec.md) and is consistent with the design goals (with some departure in implementation) at https://github.com/couchbaselabs/cbupr/blob/master/upr_design_spec.md.

## Requirements summary

EP Engine is a layer between memcached memory cache and CouchStore persistent storage.  EP Engine receives mutation events from memcached and/or clients (creation, modification, and deletion of data) and must support persistence and replication of these events.  Mutation queues manage these events.

All data is Couchbase is stored in partitions.  Mutation queues should support concurrent incoming events across partitions (serial within a partition); and concurrent outgoing events within and across partitions.

Each event contains a key (Id), a sequence number, and possibly a value.  Sequence numbers are unique and monotonically increasing within a partition.

Each outgoing event client (replicator or persister) is given an iterator into the mutation queue, beginning at its requested position (sequence number).  The iterator is coordinated with the mutation queue and is able to iterate and provide data opaquely to the outgoing event client.

## Design goals

* Maximize concurrency
* Make efficient use of memory
* Minimize memory allocation and deallocation
* Effectively support both document and counters as stored values

## Design summary

1. There is a single mutation queue per partition.  Therefore, the rest of this design applies to a single partition.

1. The EP engine already maintains a hash map per partition from key (Id) to previous value and sequence number.  This hash map will be used by the mutation queue.

1. The task for handling mutation events incoming to the queue is called a mutator.  There is a single mutator per queue.

1. A task for handling mutation events outgoing from the queue is called a duplicator.  Replicators and persisters are types of duplicators (identical clients from the queue’s perspective).  There are multiple concurrent duplicators per queue.

1. The design makes a distinction between stored values that are counters vs. those that aren’t. Here we refer to counters as counters, and to non-counters as documents (although they could be any non-counter value).

1. The queue maintains 2 separate lists of data structures: one for counters and another for documents.

1. The counter list is a list of balanced binary tree maps, while the document list is a list of fixed-size arrays.  Both the trees and the arrays are ref-counted (details below).

1. Each mutation event appends to one of these 2 lists: incr and decr append to the counter list, and all other mutations append to the document list.  Therefore, the monotonically increasing sequence numbers are interspersed between the 2 lists.

1. Both lists are searchable by sequence number in log(N) time: balanced binary trees, and (naturally) sorted arrays.

1. Given an incoming event, its previous sequence number can be retrieved from the EP engine hash map.  Given its previous sequence number, the previous mutation event for that entry can be located in the counter list or document list (both lists can be searched in combined log(N) time).

1. If a previous mutation event is found in the counter list (in a tree therein), that previous event is deleted from the tree (memory can be deallocated or recycled).  This may trigger a tree rebalancing (and likely some locking).  This prevents counters and incr / decr operations from consuming runaway memory.

1. If a previous mutation event is found in the document list (in an array therein), that event is flagged “skip”.  This is a simple boolean flag which can be stored inline or in a corresponding bit vector.  There is no locking, no structural change to the array, and no memory operation.  (The arrays also have a fixed max-size, so they never need structural changes.)  This prevents document operations from triggering excessive locking and tree maintenance.

1. The client of the outgoing events (duplicator client) cannot distinguish between a deleted event and a skipped event.  In both cases, that event will not yielded by the iterator.

1. Each iterator maintains a cursor into both lists (thus two cursors per iterator).  When returning the next event, an iterator compares the sequence numbers of both cursors and returns the lower of the two.

1. **De-duplication:** If the previous mutation event is found in the most recent data structure (tree or array) in the document list or counter list, and no duplicator has read that event, then that event is updated in place, instead of being deleted or marked skip.

## Design details
### Reference counting

Both the trees and arrays are reference counted.  Each mutator and duplicator ref-counts the tree and array that it is currently traversing.  Furthermore, each tree and array ref-counts its successor.  This prevents a structure from being collected while there’s a mutator or duplicator that may access it.

To handle very slow or non-responsive replicators (e.g. due to network issues), timers and memory thresholds could be used to supplement the ref-counting, so that structures are guaranteed to be collected.  If a replicator resumes after its structures have been collected, the replicator will be diverted to fetch from disk (backfill).

Collection always happens from the oldest structure first (because each structure ref-counts its successor).  This design doesn’t specify whether memory is deallocated or recycled into a free list.  That is an optimization detail.
