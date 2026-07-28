# DCP Statistics

DCP exposes statistics at several different scopes: cluster/bucket-wide
totals, per connection-type aggregates, per-connection detail, and
per-stream (per-vBucket) detail. Each scope answers a different kind of
question, so pick the right one for what you're trying to find out rather
than always reaching for the most detailed view.

## How to access these stats

Use the command line tool `mcstat` (see `mcstat --help` for the full set of
connection options, including how to connect over TLS/SSL via `--tls`). For
example:

```
mcstat -h <host:port> -u Administrator -P - --tls -b <bucket> dcp
mcstat -h <host:port> -u Administrator -P - --tls -b <bucket> dcpagg <separator>
```

### The following groups exists

* `dcp [json]` - per-connection and per-stream detail for every DCP
  connection on the bucket. The most detailed, and the most expensive to
  collect on a node with many connections. Instead of a plain argument, a
  JSON object may be provided to control the output, with the following
  optional fields:
  * `stream_format` - `"legacy"` (default, plain-text per-stream stats),
    `"json"` (per-stream stats as JSON), or `"skip"` (omit per-stream stats
    entirely, returning only the per-connection and aggregated stats -
    useful to cut collection cost on a node with many streams).
  * `filter` - restrict the connections included, with sub-fields `user`
    (only connections authenticated as this user) and/or `port` (only
    connections on this listening port).

  For example: `mcstat -b <bucket> dcp '{"stream_format":"json","filter":{"user":"myuser"}}'`.
* `dcpagg <separator>` - stats aggregated per connection-type (`producer`,
  `consumer`, `replication`, `views`, `xdcr`). Cheap to collect and usually
  the right starting point.
* `dcp-vbtakeover <vbid> [connection-name]` - takeover-specific stats for a
  single vBucket, used to track rebalance progress.
* `all` (or no key) - includes the bucket-wide `ep_dcp_*` totals alongside
  all other engine stats.

Run `mcstat --help=<statkey>` (e.g. `mcstat --help=dcp`) for details on a
specific stat group.

Aggregated stats (the `kv_dcp_*`/`ep_dcp_*` families) are also exposed via
Prometheus (`/_prometheusMetrics` on the bucket) for dashboards and
alerting; per-connection and per-stream stats are `mcstat`-only and are not
exported to Prometheus, since they are per-connection and not meaningful as
a long-lived time series.

`cbstats` (the Python tool previously used for the above) is deprecated in
favour of `mcstat`. It also has no support for connecting over TLS/SSL, so
it cannot be used against a node that only accepts encrypted connections.
Prefer `mcstat` for all new tooling.

## Cluster-wide DCP Totals

Bucket-wide sums across every DCP connection, regardless of type. These are
the quickest way to answer "is DCP healthy on this bucket at all?" before
drilling into a specific connection-type or connection.

| Statistic | Summary | Description |
|---|---|---|
| `ep_dcp_count` | Total number of DCP connections (producers + consumers) on the bucket. | Counts every active DCP connection on the bucket, combining consumers and producers. Check it when investigating unexpected connection counts, or before/after a rebalance. A sudden jump can indicate a client that isn't reusing connections (see [Stream naming and identification](recommended-apis.md#stream-naming-and-identification---strongly-recommended)); a drop to zero when clients are expected points to a connectivity problem needing immediate investigation. |
| `ep_dcp_consumer_count` | Number of inbound (Consumer) DCP connections. | Tracks how many inbound replication connections are open, narrowing a connection-count concern to the consumer side. Check it alongside the producer count to determine if an issue is inbound or outbound. The value should roughly track this node's replica vBucket count; an unexpectedly low count can mean replication streams failed to (re)establish after a disconnect, rebalance, or failover. |
| `ep_dcp_producer_count` | Number of outbound (Producer) DCP connections. | Tracks how many outbound DCP connections are open, covering replication, XDCR, views, and any external DCP client. Check it alongside the consumer count when narrowing down whether an anomaly affects inbound or outbound streaming. Since it aggregates several client types, an unexpected value is a signal to drill into the per-connection-type (`dcpagg`) stats to find which client is missing or duplicated. |
| `ep_dcp_total_bytes` | Total bytes sent across all currently existing DCP connections (as `dcp_total_data_size`). | The cumulative byte count sent over the wire by every DCP connection, covering replication, XDCR, backup, and indexing traffic. Look at it when investigating network/bandwidth usage attributable to DCP. A high or fast-growing value indicates heavy replication or backfill traffic; compare against `ep_dcp_total_uncompressed_data_size` to gauge how much Snappy compression is saving. |
| `ep_dcp_total_uncompressed_data_size` | Total equivalent uncompressed size of the data sent across all DCP connections. | What total DCP traffic volume would have been without Snappy compression, as an equivalent uncompressed byte count. Check it alongside `ep_dcp_total_bytes` to assess compression effectiveness for this bucket. A large gap means compression is saving substantial bandwidth; a small gap suggests one or more clients haven't negotiated `force_value_compression`. |
| `ep_dcp_queue_fill` | Number of items ever queued to send across all currently existing DCP connections. | A monotonically increasing count of every item ever queued for sending across all DCP connections. On its own it's mostly useful as the numerator for a `queue_fill - items_sent` backlog calculation. Look at it alongside `ep_dcp_items_sent`/`ep_dcp_items_remaining` to assess overall send progress and whether the bucket-wide backlog is growing or steady. |
| `ep_dcp_items_sent` | Total items sent across all DCP connections. | The cumulative count of items actually sent across every DCP connection, the natural counterpart to `ep_dcp_queue_fill`. Check it alongside that stat, and `ep_dcp_items_remaining`, for overall send progress. In a healthy cluster this should track closely with `ep_dcp_queue_fill`; a growing gap indicates a bucket-wide backlog forming, worth drilling into via `dcpagg`. |
| `ep_dcp_items_remaining` | Approximate total items still to be sent across all DCP connections. | An approximate count of items still waiting to be sent across every DCP connection, a quick bucket-wide backlog indicator. Check it after a burst of writes, a rebalance, or a failover, when backlogs are most likely. A persistently high or growing value means one or more consumers are falling behind, risking checkpoint memory growth on this node. |
| `ep_dcp_backfill_disk` | Total items queued from disk backfills across all DCP connections (as `dcp_queue_backfill_disk`). | Counts items queued for DCP delivery by reading from disk rather than the in-memory checkpoint, summed across every connection. Check it when DCP throughput looks slow, or after a rebalance/failover, both of which commonly force streams into backfill. A high value means a lot of data is being read from disk, which is slower and more I/O intensive. |
| `ep_dcp_backfill_memory` | Total items queued from memory (checkpoint) backfills across all DCP connections. | Counts items queued for DCP delivery from an in-memory checkpoint backfill rather than disk, summed across every connection. Check it alongside `ep_dcp_backfill_disk` in the same situations. High values are expected during steady-state replication; a very high value alongside memory pressure can indicate checkpoints are being held open longer than desired. |
| `ep_dcp_num_running_backfills` | Number of backfills currently running across all DCP connections. | A live count of disk or memory backfills actively running across every DCP connection. Check it when assessing disk/I/O load, or during a rebalance, when many streams may need to backfill concurrently. Compare against `ep_dcp_max_running_backfills`; if consistently at the max, new streams may be queuing before they can start backfilling. |
| `ep_dcp_max_running_backfills` | The configured cap on concurrent backfills across all DCP connections. | A static, configured value for the maximum concurrent backfills across all DCP connections, set via `dcp_backfill_in_progress_per_connection_limit` or related settings. Useful only as the denominator for `ep_dcp_num_running_backfills`. If running backfills consistently sit near this cap during catch-up-heavy periods, such as after a large rebalance, raising the limit may be worth considering. |

## Per Connection-Type Aggregate Stats (`dcpagg`)

The same shape of stat as above, but broken down per connection type
(`producer`, `consumer`, `replication`, `views`, `xdcr`). This is usually
the best starting point for monitoring, since it separates "is replication
healthy" from "is my indexer/XDCR/backup connection healthy" without having
to inspect individual connections.

| Statistic | Summary | Description |
|---|---|---|
| `connagg_producer_count` | Current number of Producer (outbound) connections of a given type. | The current number of outbound Producer connections for a specific type, such as `replication`, `xdcr`, or `views`, rather than the bucket-wide total. Check it when verifying a specific consumer type has the connection count you expect. A value of zero for a type that should be present indicates that client has disconnected or never connected. |
| `connagg_consumer_count` | Current number of Consumer (inbound replication) connections of a given type. | The current number of inbound Consumer connections by type, most commonly used for `replication`. Check it like `connagg_producer_count`, typically to confirm replication health specifically. The value should match the expected replica count for this node; a lower value indicates replication streams are missing, which will eventually show up as lagging or under-replicated vBuckets. |
| `connagg_items_sent` | Total items sent by all currently-existing streams of this connection type, since each stream was created. | Sums items sent since creation across all currently-existing streams of a given type, a per-type view of send progress. Check it alongside `connagg_items_remaining` to see if a type is keeping up. This resets when a stream is recreated (e.g. after a reconnect), so check `connagg_consumer_count`/`connagg_producer_count` for reconnects before assuming data loss. |
| `connagg_items_remaining` | Approximate items still to be sent, summed over all streams of this connection type. | An approximate count of items still queued, summed across every stream of a type, a type-scoped version of `ep_dcp_items_remaining`. Check it when a specific type, such as `xdcr` or `views`, is suspected of falling behind. A high or growing value narrows a general "DCP is behind" symptom to a specific consumer type rather than DCP as a whole. |
| `connagg_total_bytes` | Total bytes sent across all connections of this type. | Sums bytes sent across every connection of a specific type, a per-type breakdown of `ep_dcp_total_bytes`. Check it when investigating bandwidth usage by consumer type. Useful for attributing network load to a specific source, e.g. distinguishing XDCR from indexing or backup traffic; a high total narrows down which integration is responsible. |
| `connagg_total_uncompressed_data_size` | Total equivalent uncompressed size sent across all connections of this type. | What total traffic volume would have been without compression, for a given type, the type-scoped counterpart to `connagg_total_bytes`. Check it alongside that stat, per type, to assess compression effectiveness for a specific integration. A large gap indicates good compression; a small gap suggests that client hasn't negotiated Snappy. |
| `connagg_ready_queue_bytes` | Estimated memory used by items already queued to send but not yet written to the socket, for this connection type. | Estimates memory held by items queued to send but not yet written to the socket, per connection type. Check it when investigating Producer-side memory usage or a `high` memory-used alert. A high or growing value means the Producer is generating data faster than it can push onto the socket - see [Flow Control](recommended-apis.md#flow-control---strongly-recommended). |
| `connagg_paused` | Cumulative count of how many times connections of this type have been paused. | A cumulative count of how often connections of a type have paused, most often due to a full flow-control buffer. Check it alongside `connagg_unpaused` when investigating throughput problems for a type. A high, fast-growing count indicates connections of this type are frequently blocked rather than steadily streaming - dig into per-connection `paused_current_reason` next. |
| `connagg_unpaused` | Cumulative count of how many times connections of this type have been unpaused. | The cumulative count of how often connections of a type have resumed from paused, interpreted together with `connagg_paused`. Check it in the same situations as that stat. In a healthy type, this should track closely with `connagg_paused`; a growing gap (more pauses than unpauses) suggests connections are staying paused for longer stretches. |
| `connagg_items_backfilled_disk` | Number of items pushed into the DCP ready queue from a disk backfill, for this connection type. | Counts items that entered the ready queue via a disk backfill rather than memory, by connection type. Check it when determining whether a type's data is coming mostly from disk or memory. A high value for a type expected in steady-state, such as `replication`, suggests it recently had to backfill, e.g. after cursor dropping or a far-behind reconnect. |
| `connagg_items_backfilled_memory` | Number of items pushed into the DCP ready queue from an in-memory (checkpoint) backfill, for this connection type. | Counts items that entered the ready queue via an in-memory checkpoint backfill, the counterpart to `connagg_items_backfilled_disk`. Check it in the same situations as that stat. This is the expected majority source for a healthy, keeping-up type; a low value relative to the disk-backfilled count suggests that type is not keeping up with live traffic. |

## Per-Connection Stats (`dcp`)

Stats for a single, named DCP connection. Useful once an aggregate stat
has pointed at a specific consumer type and you need to find which
individual connection is the problem.

### General (Producer and Consumer)

| Statistic | Summary | Description |
|---|---|---|
| `type` | Whether this connection is a `producer` or `consumer`. | Tells you whether this connection is a Producer, sending data out, or a Consumer, receiving data in. Check it first when interpreting a connection in `mcstat -b <bucket> dcp` output, since every other stat is only meaningful once you know its role. Getting this wrong risks misreading normal values, e.g. treating an idle Consumer's lack of `items_sent` as a problem. |
| `created` | Unix timestamp of when the connection was opened. | The Unix timestamp marking when this connection was originally opened, from which its current age can be derived. Check it when checking connection age or churn, particularly if a connection behaves unexpectedly. A very recent value on a connection expected to be long-lived suggests it has been reconnecting repeatedly, worth investigating for an underlying network or client issue. |
| `pending_disconnect` | Whether the server has already decided to close this connection. | Indicates whether the server has already internally decided to close this connection, even if the socket isn't torn down yet. Check it when a connection appears stuck or unresponsive, to rule out that it's already scheduled for closure. A value of `true` means don't expect further progress; focus on why the disconnect was triggered instead. |
| `supports_ack` | Whether the connection negotiated support for acknowledged control messages. | Whether the connection negotiated support for acknowledged control messages, primarily relevant to older or non-standard clients. Check it rarely, mostly for very old client compatibility rather than routine troubleshooting. A `false` value on a modern client usually indicates a `HELO`/negotiation problem, worth checking against the client's whole negotiated feature set rather than in isolation. |
| `paused` / `paused_count` / `unpaused_count` | Whether the connection is currently paused, and cumulative pause/unpause counts. | Whether this connection is paused right now, and cumulative pause/unpause counts over its lifetime. Check them when a connection looks slow or stalled and you need per-connection detail beyond `connagg_paused`/`connagg_unpaused`. Frequent pausing (high `paused_count` relative to age) indicates regular blocking, commonly a full flow-control buffer - check `paused_current_reason` next. |
| `paused_current_reason` / `paused_current_duration` | Why the connection is currently paused, and for how long. | Why this connection is paused right now, and for how long it has stayed that way. Check them immediately after observing `paused: true`, as the natural next diagnostic step. This directly names the bottleneck, e.g. flow-control acks, a backfill, or a full ready queue, instead of requiring guesswork about where to look next. |
| `priority` | The connection's scheduling priority (`high`/`medium`/`low`). | The scheduling priority assigned to this connection, controlling how the server prioritizes it against competing DCP connections. Check it when throughput seems lower than expected for an important connection. See [Setting Connection Priority](setting-connection-priority.md) for what this controls; a `low`-priority connection among many higher-priority ones may see reduced throughput purely from scheduling. |
| `num_streams` / `num_dead_streams` (Producer only) | Total streams on this connection, and how many are no longer active. | Total streams that have existed on this Producer connection, and how many are no longer active. Check them when a Producer seems to be doing less work than expected for its vBucket count. A high `num_dead_streams` relative to `num_streams` suggests the client isn't cleaning up closed streams, or keeps failing to (re)establish them. |
| `synchronous_replication` | Whether this connection negotiated support for SyncWrites (`prepare`/`commit`/`abort` messages). | Whether this connection, Producer or Consumer, negotiated support for SyncWrites. Check it when a durable write isn't behaving as expected on a specific connection, e.g. `prepare` messages not being sent or acted on. `false` on either side means that connection won't send (Producer) or process (Consumer) `prepare`/`commit`/`abort` messages, usually fixed via the client's `HELO` negotiation. |

### Producer-specific

| Statistic | Summary | Description |
|---|---|---|
| `items_sent` / `items_remaining` | Items sent, and approximate items still to send, on this connection. | Items sent, and the approximate number still queued, for this specific Producer connection rather than any aggregate. Check them whenever this particular connection is suspected of falling behind. A high or growing `items_remaining` means this consumer specifically is behind, independent of the aggregate for its connection type - the right level of detail once an aggregate has pointed here. |
| `total_bytes_sent` / `total_uncompressed_data_size` | Bytes sent, and equivalent uncompressed size, on this connection. | Actual bytes sent, and what that would have been without compression, for this specific connection. Check them alongside the items counters to assess compression effectiveness for this client specifically. A small gap suggests this client hasn't enabled `force_value_compression`; compare against `connagg_total_bytes`/`connagg_total_uncompressed_data_size` for its type to see if it's an outlier. |
| `last_sent_time` / `last_receive_time` | Timestamps of the last message sent to, and received from, this connection. | When this connection last sent a message, and when it last received one back, a direct view of recent activity. Check them when a connection looks idle or possibly dead. A `last_receive_time` far in the past, over roughly twice the no-op interval, suggests the connection is dead and should have been caught by [Dead Connection Detection](recommended-apis.md#dead-connection-detection---strongly-recommended). |
| `noop_enabled` / `noop_tx_interval` / `noop_wait` | Whether No-Op-based dead-connection detection is enabled, its interval, and whether a No-Op response is currently outstanding. | Whether No-Op-based dead-connection detection is enabled, how often No-Ops are sent, and whether one is awaiting a response. Check them when diagnosing why a stale connection wasn't cleaned up. `noop_enabled: false` on a long-lived connection is a red flag - see [Dead Connection Detection](recommended-apis.md#dead-connection-detection---strongly-recommended); if enabled, check `noop_wait` for a stuck response. |
| `force_value_compression` | Whether the Producer is compressing values before sending. | Whether this Producer connection is compressing item values with Snappy before sending. Check it when investigating bandwidth usage for a specific connection consuming significant network capacity. A `false` value on a bandwidth-sensitive consumer, such as XDCR or backup, means that client is missing an easy win - see [Compression and Payload Size](recommended-apis.md#compression-and-payload-size---strongly-recommended). |
| `cursor_dropping` | Whether this connection has opted in to cursor dropping. | Whether this connection has opted in to cursor dropping, the mechanism letting the server drop a slow consumer's cursor to relieve memory pressure, forcing that stream to backfill later. Check it when checkpoint memory pressure seems caused by a slow consumer. `false` on a client that isn't latency-sensitive is a red flag - see [Cursor Dropping](recommended-apis.md#cursor-dropping---strongly-recommended). |
| `send_stream_end_on_client_close_stream` / `enable_expiry_opcode` / `enable_stream_id` / `synchronous_writes` / `max_marker_version` | The set of optional features this connection has negotiated. | The full set of optional DCP features this Producer connection negotiated: stream-end notifications, expiry opcodes, stream identifiers, synchronous writes, and marker versioning. Check them when confirming what a client has, or hasn't, opted into, e.g. while debugging a missing message type. Mismatches here are a common source of "why am I not seeing X" questions, usually fixed via the client's `HELO` negotiation. See also `synchronous_replication` above, which both Producer and Consumer connections report. |
| `should_disconnect_when_stuck` / `disconnect_when_stuck_timeout` | Whether the server will proactively disconnect this Producer if it makes no progress, and after how long. | Whether the server will proactively disconnect this Producer if it stops making progress, and after how long. Check them when a connection is suspected of being permanently stuck, e.g. a deadlocked client. If enabled and the timeout is reached, the server disconnects rather than leaving it wedged; expect a reconnect shortly after if the client recovers. |

### Consumer-specific

| Statistic | Summary | Description |
|---|---|---|
| `total_backoffs` | Cumulative count of times this Consumer has had to back off (e.g. because it could not keep up processing incoming messages). | A cumulative count of times this Consumer has backed off, typically because it couldn't keep up processing incoming messages. Check it when a replica connection seems slow to apply mutations relative to what's sent. A high or growing count suggests the receiving side, such as disk flushing or checkpoint processing, is the bottleneck - not the network or Producer. |
| `processor_task_state` | The current state of the background task that applies received DCP messages. | The current state of the background task that applies received DCP messages on this connection. Check it when a Consumer looks stalled and you want to know what it's actually doing. This distinguishes waiting for more data, actively processing a backlog, and being genuinely blocked - each pointing to a different next troubleshooting step. |
| `processor_notification` | Count of notifications sent to wake the processor task. | How many times the processor task on this connection has been woken up to check for new work. Check it rarely, mostly for deep debugging of internal scheduling behavior. A high notification rate alongside a low processing rate (via `last_received_seqno`/`total_backoffs`) can indicate excessive wakeups without corresponding progress, useful when reporting a suspected scheduling bug. |
| `pending_controls_size` / `pending_control` / `pending_control_value` / `pending_control_opaque` | Details of any [Control](commands/control.md) message(s) still awaiting a response. | Details of any DCP Control message this Consumer sent that's still awaiting a response - which control, its value, size, and opaque identifier. Check them when a Control message appears to never complete. A value persisting over time indicates the Producer isn't responding to it, not that the Consumer failed to send it - check Producer-side logs. |
| `pending_add_stream` | Whether an [Add Stream](commands/add-stream.md) is currently pending on this connection. | Whether an Add Stream request is currently pending, sent but not yet acknowledged, on this connection. Check it during rebalance/topology-change debugging, when Add Stream requests are commonly issued in bulk. A value stuck at `true` suggests the stream request isn't completing on the Producer side - check that vBucket's Producer-side logs for errors. |

## Flow Control Stats

See [Flow Control](recommended-apis.md#flow-control---strongly-recommended)
for the mechanism these stats describe.

| Statistic | Summary | Description |
|---|---|---|
| `flow_control` (Producer) | Whether flow control is enabled on this connection. | Whether flow control is enabled on this Producer connection, the mechanism limiting outstanding unacknowledged data. Check it first when a connection isn't moving data as expected, before the more detailed buffer stats. `disabled` means the Consumer specified a `0` buffer size - see [Flow Control](recommended-apis.md#flow-control---strongly-recommended) for when that's appropriate, since disabling it removes a safety mechanism. |
| `max_buffer_bytes` | The negotiated flow-control buffer size for this connection. | The negotiated flow-control buffer size for this connection, the max unacknowledged data the Producer may have outstanding. Check it alongside `unacked_bytes` to see how close the connection is to its limit. A very small value relative to typical item sizes can cause frequent pausing even under light load - worth comparing against actual item sizes. |
| `unacked_bytes` | Bytes the Producer has sent but which the Consumer has not yet acknowledged as processed. | Bytes the Producer has sent but the Consumer hasn't yet acknowledged, the connection's current outstanding data. Check it whenever a connection is paused or slow, since this determines if flow control is the cause. A value at or near `max_buffer_bytes` means the connection is (or is about to be) paused, waiting on a Buffer Acknowledgement from the Consumer. |
| `total_acked_bytes` | Cumulative bytes acknowledged by the Consumer over the life of the connection. | Cumulative bytes the Consumer has acknowledged over the connection's life, the counterpart to `unacked_bytes`. Check it alongside that stat to gauge throughput and confirm the Consumer is keeping pace. A value that stops growing while `items_remaining` is non-zero indicates the Consumer has stopped acknowledging entirely - a stuck client, not a Producer or network problem. |
| `last_buffer_ack_time` (Consumer) | Timestamp of the last Buffer Acknowledgement sent by this Consumer. | Timestamp of the last Buffer Acknowledgement this Consumer sent, confirming it freed up buffer space. Check it when a connection looks paused from the Producer side, to confirm the Consumer is still acknowledging. A long-stale value confirms the Consumer isn't freeing buffer space - a useful cross-check against `unacked_bytes` sitting near its maximum. |
| `flow_control_last_checked_acked_bytes` / `flow_control_last_checked_time` (Producer) | Internal bookkeeping of the last time the Producer checked its outstanding byte count. | Internal bookkeeping of when the Producer last checked its outstanding acknowledged-byte count. Check them rarely, mostly during deep debugging of flow-control internals. Mostly useful for correlating with changes in `unacked_bytes` over time; most operators won't need these day-to-day, but they help when investigating a suspected implementation bug rather than a slow client. |

## Backfill Stats (per Producer connection)

| Statistic | Summary | Description |
|---|---|---|
| `backfill_num_active` / `backfill_num_initializing` / `backfill_num_snoozing` / `backfill_num_pending` | How many backfills for this connection are actively scanning, starting up, temporarily paused, or queued waiting for a slot. | Breaks this connection's backfills down by state: scanning, starting up, temporarily paused (snoozing), or queued for a slot. Check them when this connection is behind and you want to know if backfill scheduling is the limiter. A high `backfill_num_pending` relative to the concurrency limit means this connection's streams are queuing behind others, not slow on their own. |
| `backfill_buffer_bytes_read` / `backfill_buffer_max_bytes` / `backfill_buffer_full` | Memory currently used, and the cap, for buffering backfilled-but-not-yet-sent items on this connection; and whether that buffer is full. | Memory used, and the cap, for buffering backfilled-but-not-yet-sent items on this connection, and whether that buffer is full. Check them alongside the ready-queue stats when a connection is behind and backfilling. `backfill_buffer_full: true` means the backfill is throttling itself because the Producer can't push data onto the socket fast enough - usually flow control or a slow network. |
| `backfill_order` | The configured order (`round-robin` or `sequential`) backfills run in for this connection. | The configured order this connection's backfills run in: `round-robin`, sharing progress across vBuckets, or `sequential`, finishing one before the next. Check it when investigating why some vBuckets catch up before others during heavy backfilling. See the `backfill_order` [Control](commands/control.md) option to change this if it doesn't match the catch-up priority the workload needs. |

## Per-Stream Stats (per vBucket, within a connection)

Individual stream stats, nested under each connection in `mcstat -b <bucket> dcp`
output. Use these once you've identified a specific slow or unhealthy
connection and need to know which vBucket(s) are the problem.

### Common (both directions)

| Statistic | Summary | Description |
|---|---|---|
| `state` | The stream's current state (e.g. `backfilling`, `in-memory`, `dead`). | The current state of this specific stream: `backfilling`, `in-memory`, or `dead`, the phase of its lifecycle. Check it first for any stream you're investigating, since it frames how to read every other per-stream stat. A `dead` state on a stream expected to be active means it needs re-requesting; `backfilling` vs `in-memory` tells you which other stats are relevant. |
| `start_seqno` / `vb_uuid` / `snap_start_seqno` / `snap_end_seqno` | The parameters this stream was opened (or last resumed) with. | The exact parameters this stream was opened or last resumed with: starting seqno, vBucket UUID, and snapshot boundaries. Check them when confirming a client resumed from the position you expect. A mismatch between what a client believes it sent and what these show can reveal a bug in [Stream Setup and Resumption](recommended-apis.md#stream-setup-and-resumption---required). |
| `items_ready` | Whether there are items in this stream's ready queue waiting to be written to the socket. | Whether there are items in this stream's ready queue waiting to be written to the socket. Check it when a specific stream looks stalled, to see if the server has anything queued for it. `false` for a long time on a stream expected to have data means the server has nothing new to send; `true` with no progress suggests the connection is flow-control paused. |

### Producer (Active Stream)

| Statistic | Summary | Description |
|---|---|---|
| `backfill_disk_items` / `backfill_mem_items` / `memory_phase` | Counts of items sent to this stream from disk backfill, memory backfill, and the ongoing in-memory phase, respectively. | Counts of items this stream sent from disk backfill, memory backfill, and its ongoing in-memory phase. Check them when determining whether a vBucket's stream is, or recently was, reading from disk rather than memory. A high `backfill_disk_items` count for a stream expected to be steady-state suggests a recent backfill, e.g. after cursor dropping or a far-behind reconnect. |
| `last_sent_seqno` / `last_read_seqno` / `last_sent_snap_end_seqno` | The last seqno sent to the client, the last seqno read internally, and the end of the last snapshot marker sent. | The last seqno sent to the client, last read internally, and the end of the last snapshot marker sent. Check them when diagnosing exactly how far behind a specific stream is. A growing gap between `last_read_seqno` and the vBucket's high-seqno means the stream itself is falling behind; a read-vs-sent gap instead points to a delivery bottleneck. |
| `cursor_registered` | Whether this stream currently holds a checkpoint cursor. | Whether this stream currently holds a registered checkpoint cursor, pinning checkpoints in memory until it advances past them. Check it when investigating checkpoint memory that isn't being freed. `false` on a stream not backfilling from disk is unexpected and worth investigating - it means this stream isn't pinning anything, so something else must explain the memory. |
| `takeover_since` | How long (in seconds) this stream has been in the takeover-send phase. | How long, in seconds, this stream has been in the takeover-send phase of a rebalance. Check it during a rebalance if a vBucket move seems slow. A large or growing value suggests the takeover is stuck, often because the receiving client isn't acknowledging the takeover snapshot promptly - check that client's behavior and logs next. |

### Consumer (Passive Stream)

| Statistic | Summary | Description |
|---|---|---|
| `unacked_bytes` / `last_received_seqno` | Bytes received for this stream not yet acknowledged, and the last seqno actually applied. | Bytes received for this stream not yet acknowledged, and the last seqno actually applied to this replica. Check them when a replica vBucket looks behind its active counterpart and you need per-stream detail. A `last_received_seqno` far below the active vBucket's high-seqno means this stream specifically is lagging, independent of the connection as a whole. |
| `cur_snapshot_type` / `cur_snapshot_start` / `cur_snapshot_end` | The type (`memory`/`disk`/`none`) and boundaries of the snapshot currently being received. | The type (`memory`/`disk`/`none`) and boundaries of the snapshot this stream is currently receiving. Check them when diagnosing a stream stuck partway through a snapshot. A snapshot that never closes, where `last_received_seqno` never reaches `cur_snapshot_end`, suggests the Producer stalled or disconnected mid-delivery - the fix generally needs to happen there or via a reconnect. |
| `cur_snapshot_prepare` | Whether the current snapshot contains an in-flight SyncWrite prepare. | Whether the snapshot currently being received contains an in-flight SyncWrite prepare, relevant to durable write replication. Check it when debugging durable-write replication specifically. Relevant mainly for [Synchronous Replication](recommended-apis.md#synchronous-replication-syncwrites---optional-required-for-replicadurability-aware-clients) troubleshooting, helping confirm whether the snapshot in question actually involves a SyncWrite before digging further. |

## Takeover Stats (`dcp-vbtakeover`, rebalance)

Reported per vBucket during a rebalance-driven vBucket move, and used by
the orchestrator (ns_server) to decide when a takeover is close enough to
complete.

| Statistic | Summary | Description |
|---|---|---|
| `status` | The takeover stream's current phase (`backfilling`, `calculating-item-count`, `in-memory`, `stream_is_dead`, or `connection_does_not_exist`). | The takeover stream's current phase: `backfilling`, `calculating-item-count`, `in-memory`, or `stream_is_dead`. Check it whenever a vBucket move seems slow during rebalance. `backfilling` for an extended period on a data-heavy vBucket explains the delay, since that phase scales with data volume. `stream_is_dead` means the takeover needs to be retried entirely. `connection_does_not_exist` means no DCP connection with the expected takeover name exists yet on this node - normal before the orchestrator has opened the stream, but a problem if it persists after the stream should have been created; the other stats are still reported in this case, computed directly from the vBucket rather than a live stream. |
| `estimate` | Estimated number of items still to be sent before takeover can complete. | An estimated count of items still to be sent before this vBucket's takeover can complete. Check it alongside `status` to gauge remaining time - together they show what phase it's in and how much is left. This should steadily decrease; a value that stalls or grows indicates the stream isn't progressing, worth cross-checking against `state` and recent errors. |
| `chk_items` / `vb_items` | Items remaining in the checkpoint for this stream's cursor, and total items in the vBucket. | Items remaining in the checkpoint for this stream's cursor, and total items in the vBucket. Check them when `estimate` looks unexpectedly high. A large `vb_items` means there's genuinely a lot of data to move; a large `chk_items` relative to expectations means the stream itself is behind in its own checkpoint, independent of vBucket size. |
| `on_disk_deletes` | Number of persisted tombstones (deletes) in the vBucket. | The number of persisted tombstones (deleted-but-still-tracked documents) in this vBucket. Check it rarely, mostly as context for why `estimate`/`vb_items` look higher than the "live" document count suggests. A high tombstone count inflates the data that must be moved during takeover, even though tombstones represent deleted rather than live documents clients care about. |
