
#UPR Overview (3.x)

###Motivation

The current intra-cluster replication protocol for Couchbase was designed for the needs of a clustered key-value store. As the system is designed for high throughput and low latency, it presents special challenges for reliability and consistency in the face of network and server failures, as well as slow components and occasionally connected processes. As we've added functionality like indexing and cross datacenter replication, as well as integration with other systems, like Hadoop and ElasticSearch, incremental backup, etc, these challenges become harder and more relevant and we need a well defined protocol that addresses the needs elasticity/rebalance, replicating changes and state to local cluster members and indexers and inter-cluster systems, such that the system continues to be highly available and consistent regardless of minor failures, and the ability to recover or failover to a different datacenter in cases of major failures.

Couchbase is designed to be a strongly-consistent system, such that updates, edits and deletes to documents are immediately and predictably visible to all clients of the system. Yet for performance and scalability reasons, the system uses asynchronous replication to prevent blocking and prevent slow or disconnected components from impacting the performance of the system as a whole. This means the system components must be able to reconnect to the old or a new master after either the failure of the component, or the master, and quickly determine the differences in state, resolve those differences, and resume normal operation, such that consistency and availability are achieved with little impact on performance.

###Terminology

**Application Client** - This is a normal client that performance reads, writes, updates, deletions and queries to the server cluster, usually for an interactive web application.

**UPR Client** - This is a special client that streams data from one or more Couchbase server nodes, for purposes of intra-cluster replication (to be a backup in case the master server fails), indexing (to answer queries in aggregate about the data in the whole cluster), XDCR (replicate data from one cluster to another cluster, usually located in a separate datacenter), incremental backup, and any 3rd party component that wants to index, monitor, or analyze Couchbase data in near real time, or in batch mode on a schedule.

**Server** - This is a master or replica node that serves as the network storage component of a cluster. For a given partition, only one node can be master in the cluster. If that node fails or becomes unresponsive, the cluster will select a replica node to become the new master.

**VBucket** - Couchbase splits the key space into a fixed amount of VBuckets, usually 1024. That is, keys are deterministically assigned to a VBucket, and VBuckets are assigned to nodes to balance load across the cluster.

**Sequence Number** - Each mutation (a key takes on a value or is deleted), that occurs on a VBucket is assigned a number, which should be strictly increasing as events are assigned numbers (there is no harm in skipping numbers, but they must increase), that can be used to order that event against other mutations within the same VBucket.

This does not give a cluster-wide ordering of events, but it does enable processes watching events on a VBucket to resume where they left off after a disconnect.

**VBucket Version** - A UUID, Sequence Number pair associated with a VBucket. A new version is assigned to a VBucket by the new master node any time there may have been a history branch. The UUID is a randomly generated number, and the Sequence Number is the Sequence Number that VBucket last processed at the time the Version was created.

**History Branch** - Whenever a node becomes the master node for a VBucket in the event of a failover or uncontrolled shutdown and restart, if it was not the farthest ahead of all processes watching events on that partition and starts taking mutations, it is possible it will reuse sequence numbers that other processes have already seen on this partition. This may be a History Branch, and the new master MUST assign the VBucket a new VBucket Version, so that UPR clients in the distributed system can recognize when they may have been ahead of the new master, and rollback changes at the point this happened in the stream.
If there is a controlled handover from an old master to a new master, then the sequence history cannot have branches, and there is no need to assign the VBucket being handed off a new version. This happens in the case of a rebalance for elasticity (add or remove a node) or a swap rebalance in the case of a upgrade (new version of server added to cluster, old version removed).

**Snapshot** - In order to send a client a consistent picture of the data it has, the server will take a snapshot of the state of its disk write queue or the state of its storage, depending on where it needs to read from to satisfy the client’s current requests. This snapshot should represent the exact state of the mutations it contains at the time it was taken. Using this snapshot, the server should be able to send the items that existed at the point in time the snapshot was taken, and *only* those items, in the state they were in when the snapshot was taken.
Snapshots do not imply that everything is locked or copied into a new structure. In the current Couchbase storage subsystem, snapshots are essentially “free”, the only cost is when a file is copy compacted to remove garbage and wasted space, the old file cannot be freed until all snapshot holders have released the old file. It’s also possible to “kick” a snapshot holder if the system determines the holder of the snapshot is taking too long. UPR clients that are kicked can reconnect and a new snapshot will be obtained, allowing it to restart from where it left off.

**Rollback Point** - The server will use the Failover Log to find the first possible History Branch between the last time a client was receiving mutations for a VBucket and now. The sequence number of that History Branch is the Rollback Point that is sent to the client.

**VBucket Stream** - A grouping of messages related to receiving mutations for a specific VBucket, this includes Mutation/Deletion/Expiration messages and Snapshot Marker messages. The transport layer is able to provide a way to separate and multiplex multiple streams of information for different VBuckets.

All messages between Snapshot Markers messages are considered to be one snapshot. A snapshot will only contain the recent update for any given key within the snapshot window. It may require several complete snapshots to get the current version of the document.

**Failover Log** - A list of previous known VBucket Versions for a VBucket. If a client connects to a server and was previously connected to a different version of a VBucket than that server is currently working with, this list is used to find a Rollback Point.

**Mutation** - The value a key points to has changed (create, update, delete, expire)

###Scope

The new intra-cluster replication protocol should provide an ordering of document state in a VBucket, even across cluster topology changes. This allows a client of the protocol to correctly resume from where it left off, avoiding unnecessary network load or index computation.

The protocol should also specify how to handle cases where components in the cluster disagree on the history of mutations in a VBucket.
