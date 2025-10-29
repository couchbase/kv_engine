# Throttling

Couchbase Server supports multi-tenant deployments where multiple
tenants (buckets) share the same resources on a node. By default
Couchbase uses a "first come, first served" resource allocation strategy,
which may lead to resource starvation for some tenants.  This isn't
ideal as it may lead to poor performance for some tenants when other
tenants are using a lot of resources, and in the worst situation cause
application failures due to timeouts. To ensure fair sharing of
resources, Couchbase Server should implement throttling mechanisms to
limit the resource usage of each tenant.

Note that throttling isn't necessarily related to fair sharing between
tenants, but it is also a mechanism to ensure that the node doesn't
get overloaded and that there are enough resources left for the system
to perform its internal tasks. For instance misconfigured TCP buffer
sizing and the clients requesting data faster than the network on the
node could drain causing the TCP buffers to grow without bounds and
eventually causing the node to run out of memory. Throttling would
have mitigated this issue by limiting the amount of data being sent
over the network to what the network could cope with.

The resources on a node are finite, and we need to ensure that there
are enough resources left for the system to perform its internal tasks
(e.g. replication, flushing, etc). As memcached isn't the only process
on the node, its quite hard to determine the exact amount of resources
available for the tenants. We may try to measure the resource usage of
the system and dynamically adjust the throttling parameters, but that
would be complex and error prone. In addition, the resource usage may
vary over time depending on the workload and other processes running
on the node. Therefore we will allow the operator to configure the
total capacity of the node (at least in the initial implementation?).

The resources in play are:
* CPU
* Memory
* Network bandwidth
* Disk I/O bandwidth

It sounds tempting to use "ops/sec" as the unit for throttling, but
that would be too simplistic as the resource usage of a small document
(few KB) is much lower than the resource usage of a large document
(max doc size is currently 20MB). Instead we'll use "units/sec" as the
unit for throttling.

It is hard to determine the exact resource usage of each operation,
and we can't really determine the resource usage "up front" before
executing the operation, so instead we'll account the resource usage
after the operation has been executed and throttle the next operation
if there are no more resources available.

If we look at a simple GET operation, we can assume that the operation
will consume a certain amount of CPU time, network bandwidth, and
possibly disk I/O bandwidth (if the document isn't resident). We can
then convert these resource usages into a single "units" value. For
example, we can assume that a GET operation consumes 1 unit per 4KB of
data being retrieved.  We *could* be "smarter" and have different
weights for CPU, network, and disk I/O, but that would require a lot
of tuning and testing to get right, and the weights would likely vary
between different deployments. So for simplicity, we will use a single
unit value for all operations.

A SET operation would typically consume more resources than a GET
operation due to a number of reasons. We *might* need to read the
previous document off disk, then process it and store the new value to
disk. In addition the modified document will need to be replicated to
the other nodes and use disk bandwidth there. The cost for such a
document should therefore be higher than a simple GET operation.

In the initial implementation of throttling in Elixir we set a hard
limit on the number of units per second a tenant can consume, and once
the tenant hits the limit, further operations was throttled
(i.e. delayed) until the next second when the tenant was allocated a
new batch of units. This was undesirable as it could lead to
underutilization of the node resources if some tenants weren't using
their full allocation of resources. Therefore we changed the
implementation and implemented a reservation mechanism where each
tenant is guaranteed a minimum amount of resources, and if there are
free resources on the node, tenants can use more than their reserved
amount up to a hard limit (if configured). These free resources are
allocated on a first come, first served basis.

For Elixir we implemented throttling for both read and write
operations using the same pool of resources. From looking in the PRD
it looks like one would want to separate read and write operations and
have separate pools of resources for each operation type. This would
allow us to have different reservations and limits for read and write
operations. This would also allow us to prioritize read operations
over write operations (or vice versa) by configuring higher
reservations and limits for one operation type. It does however
complicate the configuration for the operator (and I'm not sure if
most operators would want to have different settings for read and
write operations). One benefit of separating read and write operations
is that we can *guarantee* a minimum amount of write operations even
in a read-heavy workload (and vice versa).

To summarize the above, we have the following "properties" in play:

* Node capacity: The total amount of resources available on the node (may be
  unlimited, which means no throttling is performed and how things worked 
  before)
* Bucket reservation: The minimum amount of resources guaranteed for a 
  bucket (may be zero)
* Bucket hard limit: The maximum amount of resources a bucket can consume (
  may be unlimited)

## Examples

I'll add a few examples of how to configure throttling below. For
simplicity I'll use a shared R/W pool of resources.

### Example 1: Basic configuration; First come, first served

In this example we assume we set the node capacity to
"unlimited". This means that there is no throttling performed and all
tenants can use as much resources as they want. This is how Couchbase
Server worked before throttling was introduced.

### Example 2: Two buckets with reservations and hard limits

In this example we set the node capacity to 10000 units per second. We
have two buckets configured, bucketA and bucketB. Both buckets have a
reservation of 2000 units per second and a hard limit of 8000 units
per second.

With this configuration, both buckets are guaranteed to get at least
2000 units per second. Anything above 2000 units per second is
allocated on a first come, first served basis. So if both buckets are
trying to use more than their reservation at the same time, they will
have to compete for the remaining capacity on the node.

It could play out like this:

If bucketA is using 3000 units per second, bucketB can use
up to 7000 units per second (the remaining capacity on the node).

If bucketB is using 6000 units per second, bucketA can use up to 4000
units per second.

If bucketA is completely idle, bucketB can use up to 8000 units per
second (its hard limit).

### Example 3: Mix of reserved and non-reserved buckets

In this example we set the node capacity to 10000 units per second. We
have three buckets configured, bucketA, bucketB, and bucketC. BucketA
has a reservation of 3000 units per second and a hard limit of 6000
units per second.  BucketB has a reservation of 2000 units per second
and a hard limit of 5000 units per second. BucketC has no reservation
and no hard limit (i.e. it can use any remaining capacity on the
node).

With this configuration, bucketA is guaranteed to get at least 3000
units per second, bucketB is guaranteed to get at least 2000 units per
second , and bucketC can use any remaining capacity on the node.

If bucketA and bucketB are both idle, bucketC can use up to 5000 units
as the rest is reserved to the other two buckets.

# Throttling in memcached

When configured, memcached will throttle connections when a tenant
used its fair share of resources to allow other tenants to run.
We've defined a tenant as a bucket, so that you can have multiple
users logged into the system and as long as they operate on the
same bucket they're treated as a single tenant.

With the current proposal, it is possible to set a max limit on the
work performed by (all) tenants on the system, so that there are
enough "bandwidths" left on the system for it to perform its internal
tasks (e.g. persist the replication stream).

It is possible to configure a minimum "guaranteed" throughput for a
tenant by reserving a bulk of the node capacity. It is also possible
to deny a tenant to go over a certain limit.

## Unthrottled privilege

Connections holding the `Unthrottled` privilege will not be throttled,
but their usage is still accounted for and affecting throttling for
other connections.

## Problems with the current design in memcached

Memcached doesn't have an ordered execution pipe. We bind connections to
a worker thread and once connected all operations from that connection
gets executed through that worker thread. We utilize libevent to get a
notification when a connection wants to perform an operation and when
a connection gets selected, we allow the connection to execute a bulk
of commands before selecting the next connection. This design predates
a serverless deployment where we have _different_ tenants performing
operations, and we would want a fair sharing (the assumption at design
time was a single bucket deployment and the app itself should play
along)

With the current design, a tenant with 10 connections would get 10
timeslots to execute commands vs a tenant with a single connection
would get a single timeslot. One could reduce the size of the window,
but the window would always end as soon as the engine would block
(for OoO the entire pipeline would need to block), and unless one
only operates on the resident set the likelihood of blocking before
the timeslot ends is relatively high (and connections utilizing
impersonate would not be accounted for as part of this).

Changing this logic is, however, an invasive change in the core of the
system, which can't be performed in the timeframe available (as it
would require massive testing to ensure no regressions are introduced
in any corner case).

## Node properties

The node properties may be set with the (privileged) command
`SetNodeThrottleProperties` (0x2c) which takes a JSON document with the 
following syntax:

    {
      "capacity" : 25000, 
      "default_throttle_hard_limit" : 5000,
      "default_throttle_reserved_units" : 2500
    }

"capacity" is the number of units per second.

"default_throttle_hard_limit" is the hard limit of number of units per second.

"default_throttle_reserved_units" is the number of units per second reserved
for the bucket.

Note that default_throttle_hard_limit and default_throttle_reserved_units will
only affect buckets created after their value is set.

The "default" settings would be stored in
`/etc/couchbase/kv/serverless/config.json`

## Bucket properties

The per bucket properties may be set with the (privileged) command
`SetBucketThrottleProperties` (0x2a) which takes the name of the
bucket in the key field, and a JSON document with the following syntax:

    {
      "reserved" : 100,
      "hard_limit" : "unlimited"
    }

"reserved" is the guaranteed resource usage (the amount is reserved)

"Hard_limit" if present (and not set to "unlimited") the tenant will be
throttled at this limit. If no hard limit is specified the tenant will only be 
throttled unless there are no free resources on the server.

The "default" settings would be stored in
`/etc/couchbase/kv/serverless/config.json`

### Details

Memcached would continue to use its second granularity timeslots for
refilling credits to each tenant.

Memcached would calculate the per-bucket quota by assigning each bucket
the minimum quota. This must be reserved as some tenants are active at the
beginning of the timeslot and others at the end of the timeslot. The rest
of the available credits are now "distributed at a first come, first served"
base. (I am not planning to implement any kind of "fair sharing" with the 
unassigned credits over time)

Note that it would take up to a second for a new bucket to get assigned
its bucket quota, but that's assumed to be OK as it typically takes longer
than one second to create a bucket. 

From a 1000ft this would looks something like:

    if (bucket_gauge.isBelow(bucket_reserved, num_bytes)) {
       // we've not yet hit the bucket quota
       return NoThrottle;
    }

    if (!global_gauge.isBelow(node_free_capacity, num_bytes)) {
        // We're out of node capacity
        return Throttle;
    }

    if (!bucket_hard_limit || // No hard limit, or not reached it
         bucket_gauge.isBelow(bucket_hard_limit, num_bytes)) {
        return NoThrottle;
    }

    return Throttle;

#### Ideas

* At the tick interval iterate over all buckets and sum up the
  reserved resources for each bucket. The delta between total capacity
  and reserved would go into the free pool.

* Slow start buckets. For buckets without any connections, we will only
  reserve 10% of credits. Once they do get a connection, they'll
  have their reserved minimum quota at the next second (this will hurt
  folks doing "single shot" connections; but on the other side that
  is also hurting kv by spending a lot of resources doing auth)
