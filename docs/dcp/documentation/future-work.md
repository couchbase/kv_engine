## Future Work

#### Connection Filters

We should be able to filter documents in order to allow a consumer to specify a
stream where only documents matching a specific pattern are sent. Filters can
be either a regex or some sort of plugable code mechanism. We should be able to
filter on keys, values, meta data, and operation type.

#### Delta Mutations

We should be able to send only the delta of a document over DCP rather than the
whole body, both for intra-cluster replication and external consumers, since
transmitting an entire large value for a tiny edit (e.g. a single-byte append,
or most sub-doc operations) wastes bandwidth. We need a heuristic for when a
delta is actually cheaper than the full document, and a way to reconstruct the
full document from a delta when it is not resident on the replica (requiring a
background fetch). Early investigation suggests this would outperform DCP's
existing compression.

#### ActiveStream Decomposition

ActiveStream currently performs many roles at once - state machine, backfill
scheduling, checkpoint extraction, collections filtering, replica snapshot
merging, takeover negotiation, sync-replication acking, and OSO/CDC handling -
all expressed as conditional logic threaded through the same functions rather
than as separate components. We should restructure it into a pluggable pipeline
of composable stages (a core stream engine, item sources, and optional
replica-merge, filter, takeover, sync-write, OSO and CDC stages) assembled per
stream configuration, so a plain active-vbucket stream pays no overhead for
capabilities it doesn't use and each stage's invariants can be reasoned about
independently.

#### Checkpoint Ops RingBuffer

We should buffer checkpoint operations passed to ActiveStream in a ring buffer
rather than the current mechanism, to reduce allocation overhead on the
streaming path.

#### Cursor Ordering For Index/Search Streams

We should let secondary-index style consumers keep their DCP cursor behind the
fastest, rather than the slowest, replication cursor - e.g. advancing once a
mutation reaches any single replica rather than waiting for all of them - to
avoid forcing an index rollback whenever a failover promotes a replica that a
fast index cursor had already moved past. This depends on ns_server preferring
the most up-to-date replica when promoting a new active; without that guarantee
the cursor would still need to trail every replication cursor.

#### Mandatory Collections for DCP

We should require collections support for every DCP connection, rather than
treating it as opt-in. Several DCP-specific code paths already do awkward,
sometimes incorrect work to simulate a collections-free view for the few
clients that don't enable it (e.g. trimming DCP snapshots, since a
no-collections view has no seqno-advance). Since most DCP-consuming services
already support collections, the main remaining case is XDCR replicating to
pre-7.0 targets, which could enable collections but filter to the default
collection instead of disabling collections outright.

#### Dedicated Front-End Threads for Replication

DCP connections currently share the same pool of front-end threads as regular
SDK connections and rely on cooperative multitasking, which is not reliable - a
slow operation on one connection (e.g. a checkpoint-manager lock, or
large-document JSON validation) can stall others sharing that thread. Since
SyncWrites are latency-sensitive, we should move DCP replication connections
onto dedicated, OS-preemptible threads, configurable per bucket, migrating a
connection to its dedicated thread once DCP_OPEN reveals its purpose.

#### Reducing ActiveStream Lock Contention

Profiling of DCP backfill has shown significant contention on ActiveStream's
stream mutex between frontend worker threads calling `ActiveStream::next` and
AuxIO threads calling `backfillReceived`, with a meaningful fraction of thread
time lost to blocking on the lock rather than doing useful work. We should look
at removing this "big lock" from the hot path entirely, replacing it with a
concurrent queue or similar structure for handing backfilled items to the
stream.

#### Backfill Prioritization For Replication

KV caps the number of concurrent backfills and queues the rest, but a single
non-replication component opening a very large number of streams (observed with
FTS after collections) can flood that queue and delay replication backfills for
an arbitrary amount of time - visibly stalling rebalance and, if a
cursor-dropped replica can't catch up quickly enough, causing SyncWrites to
time out waiting for majority ack. We should prioritise backfills belonging to
replication streams/producers ahead of all other components' backfills.

#### Efficient Collection-Filtered Backfill on Magma

Couchstore's seqno backfill already avoids reading a document's value from disk
when it isn't needed by the requested collection filter or is already resident
in memory, but Magma's equivalent path always reads the value from disk
regardless, discarding it afterwards if it wasn't wanted. We should extend
Magma's SeqIterator interface to support conditional value reads so
collection-filtered or high-resident-ratio backfills on Magma stop paying for
disk reads they immediately discard.

#### Avoiding Redundant Disk Syncs Before Backfill

Magma flushes its memtables to disk every two minutes, so a recent disk
snapshot is normally already available, yet every new backfill currently forces
a fresh fsync of the Magma kvstore before starting - which becomes a real
bottleneck on slow disks, especially when many vbuckets request a backfill at
once (e.g. during an index build). We should start backfill from the existing
snapshot and only force a sync if the stream still needs mutations beyond what
that snapshot covers, since a newer snapshot will usually already exist by the
time that point is reached.

#### Unsorted Range Scans For Throughput

A KV Range Scan retrieving a large fraction of a bucket via the by-key index
can be slower than a brute-force seqno-order scan of the entire bucket filtered
down to the requested range, at the cost of losing key-sorted output. We should
let clients opt in to an unsorted scan mode when they want maximum throughput
over ordered results.

#### DCP Resource Quota

We should introduce a quota mechanism specifically for DCP, to bound the
resources (e.g. memory, backlog) a bucket's DCP producers/consumers can
consume, so that one connection cannot destabilise a node at the expense of
others.

#### End-to-End DCP Connection RTT

We currently lack a reliable way to measure the true latency of a DCP
connection when a proxy (e.g. the DCP proxy, running in Erlang) sits in the
middle - kernel-level socket RTT only covers the kernel-to-kernel hop and
misses time spent in the proxy layer. We should add a mechanism that tracks RTT
across the full memcached-to-memcached path so abnormal DCP connection speed
can be diagnosed with an extra data point, even though this figure won't be
directly comparable to raw socket RTT and may be non-trivial to instrument
given DCP's message snapshotting/buffering.

#### Filtering RAW Values

Many Couchbase services only operate on JSON, yet DCP always sends every
document matching the collection filter regardless of datatype - even inflating
a snappy-compressed value if the client doesn't support snappy - wasting
network and CPU on documents the consumer will discard. We should let a DCP
client opt in to server-side filtering of RAW-datatype documents, so JSON-only
consumers stop paying for data they can't use.

#### Filtering Transaction Metadata Documents

The Transactions feature maintains a fixed set of ATR metadata documents that
are updated frequently, producing DCP traffic that consumers like analytics and
indexing have no use for. We should let a DCP consumer opt in, at connection
setup, to a stream with or without these documents, filtered by their key
pattern in the producer rather than requiring every consumer to duplicate the
same filter logic.

#### Masking Transaction-Intermediate Document States

Some DCP consumers (e.g. the Kafka connector) don't want to see the
intermediate document version a transaction produces while staging a change,
whether the transaction ultimately commits (where the intermediate mutation has
no value once the final version lands) or aborts (where the intermediate
mutation logically restores the original value under a new seqno/CAS, again of
no value to a consumer that already had it). We should provide transactions
and/or DCP consumers a mechanism to mask these intermediate mutations from the
stream, distinct from the separate work to filter ATR metadata documents above.

#### Precise SyncWrite Status When Replication Is Down

When DCP has been disconnected long enough for a SyncWrite to time out, we
currently always return the generic "sync-write-ambiguous" status, even when no
ActiveStream or DCPProducer exists at all and the active durability code can
say with certainty that no DCP_PREPARE ever left the node. We should return a
more precise status in that specific case, though the write may still be
locally persisted, so it should not simply be reported as an outright failure
either.

#### Pinning Data Operations to a VBUUID

Detecting a vbucket rollback currently requires a client (e.g. Cloud Native
Gateway, for XDCR) to track VBUUIDs itself on a per-connection basis via stats.
We should let a data operation optionally carry the VBUUID it expects, likely
via a new framing extra, and have the server reject the request with a specific
error if it no longer matches the vbucket's current UUID - removing the need
for client-side UUID tracking.

#### Connection Lifetime via shared_ptr

DCP connections currently identify themselves through a hack - pinning the
first cookie on the connection as a stand-in identity for the life of the
stream - because connection ownership runs in the wrong direction for DCP's
needs (`ConnectionIface` only holds a weak reference back to the DCP handler).
We should make `Connection` shared_ptr-managed and have `ConnectionIface`
support `shared_from_this`, so `DcpProducer`/`DcpConsumer` can hold a real
shared reference to the connection instead of a pinned cookie, removing the
hack while keeping a weak-pointer-back invariant to avoid ownership cycles.

#### Preventing Promotion of Reset Replicas

In a multi-failure scenario, a replica that cannot find a valid rollback point
(e.g. due to compaction) is forced to reset to zero; if the node that was
promoted active after the first failover then also fails before that reset
replica catches back up, the next promotion can produce an active vbucket
holding no data at all, even though the data existed moments earlier on another
node. We should find a way to avoid promoting a vbucket known to have been
reset, or otherwise prevent this compounding-failure data loss.
