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
    }

"capacity" is the number of units per second

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
