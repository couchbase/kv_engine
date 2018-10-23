
#UPR Indexing (3.x)

###Criteria and process for triggering index updates

When an index is first built, it gets a snapshot for each partition (couchstore database), reads all their documents, runs the map function against each document to get a list of Key/Value pairs for each document and inserts them into a b+tree, which ends up calling the reduce function. At the end of this process, the index will store in its header the sequence number of each partition.

To determine when an incremental index update is needed (because new documents were added or existing documents were updated or deleted), the indexer needs to know what the current sequence number for each partition is. If any of the current sequence numbers is higher
than the one the indexer has last seen (and stored in its header), then it knows there are new changes (mutations) to process. Up to Couchbase Server 2.2, the indexer (like XDCR) would grab a snapshot of the databases and start traversing their the sequence b+tree from the last
sequence number it processed (stored in its header) until it reaches the end of the snapshot. This allows the indexer (and XDCR) to know the IDs of the new (or updated/deleted) documents, their revision, location of their contents and other metadata.

With UPR, the indexers will no longer directly get the current sequence numbers from the database files nor will they directly traverse their sequence b+trees to get information about new mutations and document bodies. Instead they’ll get all this information from the UPR server, via a network connection. This has the benefit that documents for many mutations (especially recent ones) will be served from memory and require no disk access. Also, because the indexers no longer needs to interact with database files directly, it means indexers can run in nodes other than the datastore nodes, allowing for a different indexing topology.

###Indexing during steady state

The indexer will establish a connection to the UPR server and tells it:

* Which partitions it’s interested in
* The last partition version it has seen (see the UPR specification for details about this)
* The sequence number for each partition, after which the UPR server should start sending mutation notifications and document bodies
* The squence number where the stream should end

From this point on, the UPR server starts sending all the information about mutations and their corresponding document bodies, and the indexer just does the same work as it used to do before the UPR-era.

#####Conditions that UPR streams must respect

An UPR snapshot contains at most one notification related to that document (creation, update, delete). Snaphots are sent in order, i.e. that a mutation in a previous snapshot happened before the mutation in the current one.

#####Dealing with snapshots

One snapshot always contains exactly one mutation of every item within the partition. Those changes are applied to the index in the same order as they are supplied by the UPR stream.
The UPR stream is a multiplexed stream that contains information about all the partitions that were requested. There is one snapshot per partition transmitted, the partitions are looped through in round-robin fashion. In case there are new updates after it was looped through all partitions, the indexer will again receive a snapshot of all partitions.

###Indexing during rebalance

During rebalance, because partitions are being moved between nodes, there are two types of index inconsistency problems that can happen:

* Missing rows. A node can’t be made the new master of a partition until all its data has been transferred from the previous master to the future master and until the index on the future master has caught up with the previous master. If this is not respected, and even if no mutations are happening during rebalance, two consecutive results for the same query could miss rows originating from that partition (that is, return different results).
* Duplicated rows. At any time during rebalance, a partition can only marked as active in an index in exactly one node, no more and no less.

To guarantee the first requirement, the cluster management layer, after all data was transferred from the current master to the future master of the partition, tells the index at the current master node to stop indexing new changes from that partition and waits for the index at the future master to have caught up (regarding that partition) with the index at the current master (that is, both have processed up to the same sequence number). Once this condition is met, it tells the future master (now current master) to transition that partition to active state and mark it for cleanup in the current master (now old master).

To guarantee the second requirement, the cluster management layer, at any node knows exactly in which node each partition is active. When a query arrives at a node, the cluster management layer passes a map of active partitions to the query engine. This map associates nodes to lists of partitions that each node must contribute to for the query execution. The query engine will then not stream results (rows) associated to any partition that is a not a member of the corresponding list. This way it is guaranteed that even if one partition is found in the index of more than one node, we don’t get duplicate rows in the query results. However filtering data from partitions that are not part of the list supplied by the cluster manager has some cost for reduce views, as it implies some reductions can not be used because values from excluded partitions have contributed to the computation of the reductions. This means we have to go down the tree, exclude values from some partitions and call the reduce function against all other values, propagate this value up the tree to run rereduce operations as necessary. In the very worst case this means visiting all the b+tree nodes of an index. In practice however, this worst case has been observed rarely and query latencies haven’t gone beyond hundreds of milliseconds.

Further, to satisfy both requirements, if a node misses any partition listed in the list supplied by the cluster manager at query time, it raises an exception and causes the top level query handler in the cluster manager to retry the query up to N times, with some short wait periods between each retry. This need arises due to the masterless and peer-to-peer nature of Couchbase Server clusters. In practice we rarely see these retries happening, and when they do, they have not been more than one or two.

###Indexing during and right after a failover

#####Indexer learns about the failover

The indexer doesn’t get a notification that a failover happened, it rather gets instructions from the cluster manager on what to do. This means the cluster manager tells the indexer that it is now responsible for a different set of partitions. The indexer acts accordingly and removes the partitions it’s no longer responsible for. It also stops the UPR stream and will create a new one with the partitions it should now index.

#####Clean failover

The storage as well as the indexer activate the replica partitions. The indexer then starts the new connection to the UPR server with the updated list of partitions. No rollback is needed and the indexing can just proceed.

#####Node rejoining the cluster

A node has failed over but then rejoins the cluster (e.g. it was rebooted for some reason). It still contains the same set of partitions as before the failover, so the cluster manager might decide this node is going to be again the master of those partitions. If this happens, the indexer might have previously indexed items that were not persisted to disk before the failover - now the indexer is ahead of the storage, that is, inconsistent. See the “Indexer is ahead of the datastore” section for information about how to solve this inconsistency.

###Indexer is ahead of the datastore

#####Rolling back to a point back in time that is consistent with current datastore state

The UPR ROLLBACK response contains the sequence number to which point a partition in the index should be rolled back to. The indexer will find an older header (checkpoint) that contains a sequence number that is lower than or equal to the sequence number supplied by the ROLLBACK response. Once such header is found, the file is truncated to the position that matches the end of that header, discarding all data indexed after that checkpoint.
A new UPR connection will be instantiated, which sends the partition versions from the header we found to the UPR server. Now the index isn’t ahead of the storage any more and the response should be OK. The indexing now proceeds the normal way until it has caught up with the datastore, and queries can now be served.
In the most extreme scenario no such past header is found - in this case the whole index is discarded and has to be fully rebuilt - this is not expected to happen frequently.

#####Rollback and index compaction

The compaction process consists of taking the most recent index snapshot and move all its data into a new file. Its result will be a much smaller file, which has no fragmentation and has perfectly balanced b+trees. This means that the index file it produces has only one header (checkpoint), and if a rollback demands a lower sequence number for any partition, the index has to be fully rebuilt.

To minimize the frequency of this case (which can only happen when a node restarts), a fragmented index file will not be replaced with the file produced by the compactor if at the moment the indexer is running and is attempting to do a rollback. Once the indexer knows it doesn’t need to do a rollback, it unblocks the compactor. Shall it need to perform a rollback, it unblocks the compactor once it finishes the rollback and it tells the compactor to work on the snapshot that satisfies the rollback.

#####Avoiding full index rebuilds on rollback

We believe it’s possible to avoid full index rebuilds when a rollback is needed. Because cleanup of data coming from a specific group of partitions and adding data from other partitions is currently possible in the same b+tree operation (not going down the same paths more than once), as this is currently used for the rebalance scenario. Further we can enhance this existing
operation in such a way that it performs cleanup of existing data from a group of partitions and adds new data from those partitions in a single step, with the cleanup happening before adding new data. This will be an optimization left for a much later phase, and not necessary from a correctness/consistency point of view.

###Querying the index (stale options)

When querying the index there are several options for the consistency. There are currently two basic options: either return results immediately without caring which items got already indexed (stale=ok). Or wait until a certain partition version before the query returns (details can be found in the “Read Your Own Write” specification).
When querying with a certain partition version the request might block forever, when the requested partition version is never seen by the indexer. That can happen if a node goes down, before it’s item got replicated to other instances. As the cluster manager will notice this failure, the indexer will respond with an error, that the request couldn’t be fulfilled.

#####Purging

It can happen that items got deleted and then purged before the indexer had the chance to known about the fact they were deleted. In that case the index needs to be rebuilt from scratch.


###Dedicated indexing topology and UPR

Given that UPR’s transport is a network connection (TCP), this allows for having indexers running on nodes other than the ones where the main storage is located, offering much better scalability and performance. This offers several important advantages over the current topology where every storage node also runs indexers:

* Scatter gather now covers a subset of the cluster, instead of all the nodes in the cluster. This is very important because view keys are spread over all the indexing the nodes and view responses are sorted by key - this means that the node that receives a query (let’s call it the merger node) as to propagate the query to all other nodes in the cluster and merge its (partial) results with the query results (partial too) from all other nodes - in other words, query latency is determined by the slowest node in the cluster.
* By having the possibility of having dedicated nodes for indexing, there will be less interference between indexing and other major features, such as KV use case and XDCR. In other words, there will be less resource contention when using multiples features from Couchbase Server and the indexing nodes can be tuned for better indexing performance

This would require changes to the cluster management layer however, as it would need to be aware of two distinct types of nodes now: indexing nodes and storage nodes. Rebalancing would then be separate. So new data nodes can be added without having an impact on the indexer nodes.

However by having the indexers running on dedicated nodes, network traffic will be higher and there might be extra latency in getting data from the storage machines, therefore having an impact on non-stale queries.
