# Recommended APIs

This page describes the set of DCP messages and [Control](commands/control.md)
flags that a production-quality DCP client (an indexer, replicator, backup
tool, or other third-party integration) should implement. [Building a simple
client](building-a-simple-client.md) covers the bare minimum needed to stream
data from a single, always-healthy node. Real deployments have to tolerate
rebalances, failovers, slow consumers and long-lived connections, so this page
lists the additional APIs that are recommended and in some cases required to
build a client that behaves well in production.

Recommendations are grouped by category and marked as:

* **Required** - the client will not work correctly without this.
* **Strongly recommended** - the client will work without it, but will be
  fragile, inefficient, or unsupported in production topologies.
* **Optional** - only relevant to specific use cases (e.g. sync writes,
  collections).

## Connection Setup

### Open Connection - Required

Every client must send [Open Connection](commands/open-connection.md) to
create a Producer (or Consumer) connection.

* Name the connection `service-name:unique-information` (see
  [Open Connection](commands/open-connection.md)) so that the connection can
  be identified in `stats dcp` output and correlated with client-side logs
  during a post-mortem. Re-using a name will disconnect any existing
  connection using that name, so the `unique-information` portion should be
  globally unique (e.g. include a monotonic counter or timestamp).
* Negotiate features with [`HELO`](../../BinaryProtocol.md#0x1f-helo) before
  opening the DCP connection - it is a prerequisite for using
  `force_value_compression` (below) and other datatype-related features.

### Stream naming and identification - Strongly recommended

Use one DCP connection per logical consumer, and prefer many streams
(one per vBucket, or `sid`-qualified streams, see
[Enable Stream ID](commands/control.md)) over many connections. Opening a
large number of connections does not scale and is explicitly called out as a
non-goal in [Building a simple client](building-a-simple-client.md).

## Dead Connection Detection - Strongly recommended

Long-lived DCP connections can silently die (particularly across a WAN or
through a proxy) without either endpoint's TCP stack noticing. Clients should
enable [No-Op](commands/no-op.md)-based
[dead connection detection](dead-connections.md):

1. Send [Control](commands/control.md) `enable_noop` = `true`.
2. Send [Control](commands/control.md) `set_noop_interval` = `120` (a 120
   second interval is recommended; it must always be set alongside
   `enable_noop` so both sides agree on the interval).
3. Respond to every [No-Op](commands/no-op.md) sent by the Producer
   immediately - if the Consumer does not respond within one noop interval,
   the Producer will disconnect.
4. Independently, if the client hasn't seen *any* message (data or No-Op) for
   two noop intervals, it should treat the connection as dead and reconnect.

## Flow Control - Strongly recommended

Without flow control a Producer can send data faster than a Consumer can
process it, causing unbounded memory growth on the Consumer. Clients should
implement connection-level [flow control](flow-control.md):

* Send [Control](commands/control.md) `connection_buffer_size` with the
  number of bytes the client is willing to buffer. A non-zero value is
  strongly recommended for any client that is not certain it can drain the
  stream as fast as the Producer can produce; `0` disables flow control
  entirely and should only be used for trusted, low-volume, same-node
  connections.
* Send [Buffer Acknowledgement](commands/buffer-ack.md) once the client has
  freed up space in its buffer. As a guideline, acknowledge after 50KB or 20%
  of the buffer has been processed, whichever comes first (see
  [Consumer-Side Buffer Advertising](flow-control.md#consumer-side-buffer-advertising)).
* Be prepared to receive a single item that is larger than the advertised
  buffer size - Couchbase items can be up to 20MB and the Producer will still
  send an over-sized item rather than deadlock (see
  [Large items](flow-control.md#large-items)).
* Stream (per-stream) flow control is not supported by Couchbase Server;
  only connection-level flow control needs to be implemented.
* Without flow control (and relying on TCP's flow control by not reading from
  the socket) would makes the connection vulnerable to disconnection from
  TCP-layer timeouts (we're using`TCP_USER_TIMEOUT`).

## Cursor Dropping - Strongly recommended

Send [Control](commands/control.md) `supports_cursor_dropping` = `true`. This
tells the server the client can tolerate having its DCP cursor dropped (and
the stream subsequently resumed from disk) if the client is not reading fast
enough. Couchbase Server highly recommends enabling this in all situations;
it protects overall cluster memory health at the cost of a potential backfill
for a slow client, rather than letting a slow client stall checkpoint/cursor
cleanup for everyone else.

## Stream Setup and Resumption - Required

* Use [Get All vBucket Sequence Numbers](commands/get_seqno.md) at startup (or
  after a topology change) to discover the current high-seqno per vBucket,
  rather than assuming a fixed vBucket count/state.
* Use [Failover Log Request](commands/failover-log.md) (or persist the
  failover log delivered on a successful
  [Stream Request](commands/stream-request.md)) to keep track of the
  vBucket-UUID/seqno pairs needed to resume correctly and to pick the right
  vBucket-UUID after a rollback.
* Persist `Last Received Seqno`, `Last Snapshot Start Seqno`, and
  `Last Snapshot End Seqno`, and resume streams using the invariant
  `Snapshot Start Seqno <= Start Seqno <= Snapshot End Seqno`. See
  [Restarting from where you left off](building-a-simple-client.md#restarting-from-where-you-left-off).
* Implement the [rollback](rollback.md) response and prefer rolling back to
  the nearest snapshot in the retained history over always rolling back to
  0, since rolling back to 0 forces a full backfill.
* Prefer the `0x04` (**To Latest**) or `0x10` (**Active VB Only**) flags on
  [Stream Request](commands/stream-request.md)/[Add Stream](commands/add-stream.md)
  where applicable, so the server enforces the client's intent (e.g. failing
  fast with `NOT_MY_VBUCKET` rather than silently streaming from a
  non-active vBucket).
* Send [`enable_expiry_opcode`](commands/control.md) = `true` if the client
  needs to distinguish an explicit delete from a TTL-based
  [Expiration](commands/expiration.md); otherwise expired documents are
  reported as ordinary [Deletion](commands/deletion.md) messages. See
  [DCP Expiry Opcode Output](expiry-opcode-output.md) for the caveats around
  documents that expire before persistence.
* Send [Close Stream](commands/close-stream.md) to explicitly stop a stream
  the client no longer needs (e.g. on vBucket ownership change), and enable
  [`send_stream_end_on_client_close_stream`](commands/control.md) so the
  server confirms the stream is fully closed with a
  [Stream End](commands/stream-end.md) message instead of leaving in-flight
  messages ambiguous.

## Compression and Payload Size - Strongly recommended

* Negotiate Snappy via [`HELO`](../../BinaryProtocol.md#0x1f-helo) and then
  send [Control](commands/control.md) `force_value_compression` = `true` so
  mutation/deletion values are transmitted compressed. This significantly
  reduces bandwidth for replication-style consumers (XDCR, backup) that don't
  need to inspect the value.
* If the client only needs keys and metadata (e.g. an indexer that tracks
  presence, not content), set the __No value__ (`0x8`) flag on
  [Open Connection](commands/open-connection.md) rather than discarding the
  value after receiving it - this saves both server and network work. Add
  `0x40` (**No value with underlying datatype**) if the client still needs to
  know the original datatype of the stripped value.
* Set the __Include XATTRs__ (`0x4`) flag on
  [Open Connection](commands/open-connection.md) only if the client actually
  consumes XATTRs (e.g. XDCR); leaving it unset avoids transmitting metadata
  the client will not use.

## Out of Order Backfills - Optional (recommended for backfill-heavy consumers)

Send [Control](commands/control.md) `enable_out_of_order_snapshots` =
`true_with_seqno_advanced` (preferred over plain `true`) so the server may
use [OSO Snapshot](commands/oso_snapshot.md) messages during backfill. This
allows the server to stream a vBucket's contents in whatever order is
cheapest to read from disk rather than strict seqno order, which can
significantly speed up the initial backfill for large vBuckets. Clients that
enable this must:

* Track the greatest seqno seen during the OSO snapshot (`X`) and the
  greatest seqno seen before it started (`Y`).
* Resume from `Y` if the connection drops mid-snapshot, or from `X` (or the
  seqno carried by a trailing [Seqno Advanced](commands/seqno-advanced.md)
  message, if `true_with_seqno_advanced` was requested) once the snapshot's
  end has been received.

Also handle [Seqno Advanced](commands/seqno-advanced.md) outside of OSO use -
it is sent whenever the vBucket's high-seqno moves past an event the client
isn't subscribed to (e.g. a sync-write prepare, or a mutation in a collection
the client didn't request), and must be used to correctly detect that a
snapshot is complete.

## Collections - Optional (required for any collection-aware client)

* Enable collections during [Open Connection](commands/open-connection.md) to
  receive data for non-default collections.
* Use the [`collections` or `scope` key](commands/stream-request-value.md) in
  the stream-request value to filter a stream to specific collections, rather
  than filtering client-side after receiving every mutation.
* Track [System Event](commands/system_event.md) messages (collection/scope
  create, drop, modify) and persist the manifest-UID; include it via the
  [`uid` key](commands/stream-request-value.md#uid) whenever resuming a
  stream, per [Stream Request](commands/stream-request.md).
* Enable `flatbuffers_system_events` via
  [Control](commands/control.md) to receive collection/scope metadata
  modification events, not just create/drop.
* Consider enabling `max_marker_version=2.2` via
  [Control](commands/control.md) and sending `purge_seqno` in the
  stream-request value (see [`purge_seqno`](commands/stream-request-value.md#purge-seqno))
  to reduce unnecessary rollbacks caused solely by tombstone purging.

## Synchronous Replication (SyncWrites) - Optional (required for replica/durability-aware clients)

Clients that participate in durable writes (e.g. cross-node replica
connections) must handle:

* [Prepare](commands/prepare.md), [Commit](commands/commit.md), and
  [Abort](commands/abort.md) messages in addition to ordinary mutations and
  deletions.
* [Seqno Acknowledged](commands/seqno-acknowledged.md), sent by the Consumer
  to report the highest prepared seqno back to the Producer for durability
  tracking. This requires setting `consumer_name` and `enable_sync_writes` =
  `true` via [Control](commands/control.md) first.

## Priority and Compatibility - Optional

* Use [Control](commands/control.md) `set_priority` to raise the priority of
  latency-sensitive connections (e.g. indexing) relative to background/bulk
  consumers.
* Send [Control](commands/control.md) `v7_dcp_status_codes` = `true` so the
  client can correctly interpret the `DcpStreamNotFound` (`0x0A`) and
  `OpaqueNoMatch` (`0x0B`) status codes introduced in Couchbase 7.0, instead
  of treating them as generic errors.
* Set `backfill_order` via [Control](commands/control.md) if the client has a
  preference between `round-robin` (fairer across many vBuckets) and
  `sequential` (faster time-to-completion per vBucket) backfill ordering.

## Summary Checklist

For a client intended for production use against a multi-node cluster, at a
minimum implement:

- [ ] [Open Connection](commands/open-connection.md) with a unique,
  supportable connection name
- [ ] `enable_noop` + `set_noop_interval` ([dead connection detection](dead-connections.md))
- [ ] `connection_buffer_size` + [Buffer Acknowledgement](commands/buffer-ack.md) ([flow control](flow-control.md))
- [ ] `supports_cursor_dropping` = `true`
- [ ] [Get All vBucket Sequence Numbers](commands/get_seqno.md) and
  [Failover Log Request](commands/failover-log.md) for startup/resumption
- [ ] Persisted snapshot/seqno state and correct
  [rollback](rollback.md) handling
- [ ] [Close Stream](commands/close-stream.md) /
  [Stream End](commands/stream-end.md) handling for topology changes

Everything else in this document should be adopted based on the specific
integration's needs (collections, durability, compression, backfill
performance).
