# Setting Connection Priority

DCP connection priority controls how much of a front-end worker thread's
attention a connection receives when multiple connections share that
thread. Concretely, it sets `max_reqs_per_event` - the number of DCP
messages the server will write to that connection's socket in a single pass
of the event loop before yielding to service other connections on the same
thread.

## Setting the priority

There is no priority field in [Open Connection](commands/open-connection.md)
itself. Every new DCP Producer connection defaults to `medium` priority when
created. The client changes it afterward, at any point on an already-open
connection, via a [Control](commands/control.md) message:

* `set_priority` = `"high"`
* `set_priority` = `"medium"`
* `set_priority` = `"low"`

Separately, the server will automatically raise certain internal connection
types (for example, connections used for intra-cluster replication) to
`high` priority when their connection type is established, regardless of
whether the client ever sends `set_priority` itself.

## What priority actually controls

Priority maps directly to `max_reqs_per_event`, the number of DCP messages
serviced per connection on each pass of the front-end thread's event loop
before it moves on to service other connections. The default batch sizes
per tier are:

| Priority | Messages per event-loop pass |
|----------|-------------------------------|
| `high`   | 50                            |
| `medium` | 5 (the DCP default)           |
| `low`    | 1                             |

For comparison, an ordinary, non-DCP client connection (KV gets/sets, etc.)
uses a default of `20` messages per pass.

One important factor here is: the front end will service up to
`max_reqs_per_event` messages for a connection in a single pass, unless it
chooses to stop servicing the connection sooner due to:

* the send queue exceeding the threshold (1 MB);
* the next message not being available immediately;
* the connection having used up its timeslice.

The difference between tiers is substantial, not cosmetic. Notably, a DCP
connection left at the default `medium` priority is serviced in *smaller*
batches than an ordinary KV client connection sharing the same front-end
thread - so an indexer, replicator, or backup connection that never sets its
own priority can end up relatively starved behind regular KV traffic on a
busy node.

## Recommendations

* Set `high` for latency-sensitive consumers - for example, an indexer that
  needs to stay close to real-time, or a replica/consumer connection that
  must keep up with its active vBucket to avoid falling behind - so they are
  serviced in large batches even when the front-end thread is busy with
  other connections.
* Set `low` for bulk or best-effort consumers (for example, backup jobs or
  ad-hoc tooling) that should not compete with more time-sensitive traffic
  sharing the same front-end thread.
* Leave connections at `medium` only when there is no strong latency or
  throughput requirement either way.

Getting this wrong has a real cost in both directions: leaving a
latency-sensitive connection at the `medium` default risks it being starved
behind other connections when a thread is busy, while raising a bulk
consumer to `high` unnecessarily can crowd out more important traffic
sharing that thread.
