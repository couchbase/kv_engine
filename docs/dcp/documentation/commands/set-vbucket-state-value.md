# Set VBucket State (0x5b) Value Specification

Sent by ns_server to the producer specifying the additional metadata required
to set the vBucket state. The command can optionally accept a value, which
encodes a JSON object to configure the vBucket.

The following keys can be include in the JSOn object.

* `topology` - for setting a replication topology required for Synchronous
               Replication

* `use_snapshot` - for creating a vbucket from an existing snapshot (File-Based rebalance or Fusion)

* `next_expected_state` - to help optimise rebalance

## Key Definition

### topology

`topology` should be set by ns_server to tell the producer memcached about a
given replication topology. This `topology` is used to determine which nodes
must ack DCP_PREPARES as part of Synchronous Replication to satisfy durable
writes. This field should only be sent to active vBuckets.

A `topology` consists of an array of replication chains. A replication chain
consists of a series of string values that correspond to the names of memcached
nodes. The first name in each replication chain should correspond to the active
node. A replication chain could have a null value instead of a string to
indicate that there is an undefined replica (i.e. we may have 1 replica
configured but a failover will cause that replica to become undefined or null).

A `topology` may contain up to two replication chains. The first chain is the
current replication `topology`. The second chain is used during the later stages
of rebalance to tell memcached about the post-rebalance `topology` so that
memcached can ensure durable writes are satisfied with the new `topology` too.

For example:

A replication topology with a single chain with active and one replica
```
{
    "topology" : [[<active_name>, <replica_name>]]
}
```

A replication topology with a single chain with active and one undefined replica
```
{
    "topology" : [[<active_name>, null]]
}
```

A replication topology with two chains
```
{
    "topology" : [[<active_name>, <replica_name>],[<new_active_name>, <new_replica_name>]]
}
```

### use_snapshot

Accepts the string value "fbr" or "fusion". In these cases the vbucket will not be created in
the empty/epoch state, instead the vbucket will use a disk snapshot to seed the creation.

In this case the vbucket creation will run asynchronously as disk I/O is required to read the
snapshot.

### next_expected_state

Accepts the string values "active", "replica", "pending" or "dead". This exists primarily for
rebalance when a vbucket is first created as a replica, but may (provided no errors) switch
to a new state, for example this vbucket will become an active once rebalance has moved data.
This permits kv-engine to better manage the cache, for example priortising vbuckets that will
become active and ensuring they get enough cache space.
