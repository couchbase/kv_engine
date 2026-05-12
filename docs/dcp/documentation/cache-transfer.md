# Cache Transfer

Cache Transfer is an optional phase of a DCP stream that lets a producer copy
the resident contents of an active vbucket's hash-table to the consumer before
"regular" DCP streaming begins. Its purpose is to warm the consumer's cache
without the consumer having to read from disk.

The typical use-case is file-based rebalance or similar features where vbucket
data is moved outside of regular DCP. In FBR a vbucket is moved to a new node by
transferring the on-disk files directly, so the destination has data but an
empty cache. The destination opens a DCP stream with the[CacheTransfer]
(commands/stream-request.md#cache-transfer-option) flag, the source visits its
hash-table and ships the eligible resident items batched inside [DcpCacheTransfer]
(commands/cache-transfer.md) messages. When the visit completes a
[DcpCacheTransferEnd](commands/cache-transfer-end.md) message is sent and
(if requested) the stream switches into a regular ActiveStream so that
mutations newer than the cache-transfer phase continue to flow.

There are no ordering guarantees and no snapshot semantics during the cache
transfer phase: items can arrive in any order and a consumer must not treat the
sequence numbers contained in `DcpCacheTransfer` messages as a snapshot.

## Configuring a Cache Transfer

A Cache Transfer is opted-in by setting the `CacheTransfer` flag (`0x100`) in
the [Stream Request](commands/stream-request.md). The stream's
`start-seqno` is reused as the upper-bound seqno for the cache transfer (items
with `by_seqno <= start-seqno` are considered eligible for transfer).

* `end_seqno == start_seqno` - only the cache transfer is performed. When the
  visit completes the stream ends with a normal
  [DcpStreamEnd](commands/stream-end.md) message.
* `end_seqno > start_seqno`  - cache transfer is performed first, then a
  [DcpCacheTransferEnd](commands/cache-transfer-end.md) message is sent and
  the stream transitions to a regular ActiveStream which will deliver items
  with `by_seqno > start-seqno` as normal mutations / deletions / etc.
  inside [DcpSnapshot](commands/snapshot-marker.md) markers.

The Stream Request's value (JSON) may contain a `cts` object that customises
the transfer - see
[stream-request-value.md#cts](commands/stream-request-value.md#cts).

## What is eligible for transfer?

The producer visits the active vbucket's hash-table. A StoredValue is sent only
if all of the following hold:

1. It is not a temporary, deleted or pending item.
2. It is not in a dropped collection.
3. `by_seqno <= start_seqno` of the stream request.
4. It passes the connection's collection filter.
5. If the value is being transferred (see below), the value is resident in
   memory and not expired.

Items that do not satisfy the resident/expired check are still sent as
key+metadata if the request was for `all_keys` (see below).

## Sending the value vs. sending only the key

The `cts.all_keys` option allows the consumer to control whether values are
transferred or only the key+metadata. Cache Transfer always tries to do the
most useful thing within the constraints supplied by the consumer:

* `all_keys == false` (default): only resident, non-expired values are
  shipped. A non-resident or expired item is skipped entirely. If the
  consumer's reported `free_memory` is exhausted the producer stops the
  transfer.
* `all_keys == true`: every eligible item is shipped, but the producer is
  free to downgrade to "key + metadata only" (datatype is preserved as
  the underlying document datatype) at any point. The downgrade is triggered
  by either:
    1. the consumer's `free_memory` budget being exceeded, or
    2. the producer being above its own high-water-mark threshold.

Consumers must therefore always be prepared to receive a `DcpCacheTransfer`
message that has `value_len == 0`.

## Flow control & batching

Multiple items are packed into a single `DcpCacheTransfer` message up to a
producer-side configured byte limit (`dcp_cache_transfer_max_batch_bytes`).
A message therefore contains one or more
`[DcpCacheTransferPayload][key][value]` records back-to-back - the wire
format is parsed using
[cb::mcbp::DcpCacheTransferBuffer](../../../include/mcbp/protocol/dcp_cache_transfer_buffer.h).

DCP flow control accounts for the whole message: the sum of every
`DcpCacheTransferPayload + key + value`.

The `DcpCacheTransferEnd` message is also flow-controlled, but that is a fixed size
message (24 bytes).

## Cache hint

Each transferred item carries an opaque `cacheHint` byte alongside the
metadata. The producer copies the StoredValue's frequency-counter (MFU /
NRU-style eviction hint) into this byte so the consumer can seed the hint on
the new StoredValue and preserve eviction behaviour. The consumer treats the
byte as opaque - the meaning is private to the active producer/consumer pair.

## Memory pressure & cancellation

The CacheTransferTask runs incrementally on the executor pool. Each step:

* yields and reschedules when the producer is above the backfill threshold
  and `all_keys` is set (the transfer is deferred until memory recovers);
* cancels the transfer entirely when the producer is above the backfill
  threshold and `all_keys` is not set; the stream then ends as if the cache
  transfer phase had completed normally (so the consumer may still receive
  the ActiveStream phase if `end_seqno > start_seqno`).

The consumer-supplied `cts.free_memory` is a best-effort soft budget. As items
are queued the producer decrements its local copy; when the budget is
exhausted with `all_keys == false` the transfer ends; with `all_keys == true`
the producer switches to key-only mode and continues.

## Stream state machine

```
                       CacheTransferEnd
   Active  ───────────────────────────────►  SwitchingToActiveStream
     │                                           │
     │ end_seqno == start_seqno (stream done)    │ ActiveStream created
     ▼                                           ▼
    Dead  ◄──────────────────────────────────  Active (regular DCP)
```

## See also

* [Stream Request (0x53)](commands/stream-request.md) - how to enable the
  CacheTransfer phase.
* [Stream Request Value JSON](commands/stream-request-value.md) - the `cts`
  options.
* [DcpCacheTransfer (0x66)](commands/cache-transfer.md) - the per-batch
  message wire format.
* [DcpCacheTransferEnd (0x67)](commands/cache-transfer-end.md) - the
  end-of-cache-transfer marker.
