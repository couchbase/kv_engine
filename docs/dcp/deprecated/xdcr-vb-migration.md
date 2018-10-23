## Overview

This design doc is to explain possible optimization in XDCR under UPR when vbucket (partition) migration
happens due to failover or rebalance.


## VBucket replicator restart on migration

A bit background on how XDCR works. Today each vbucket has its own replicator responsible for replicating all mutations in that vbucket, on their arrival order.  But during topology change, which can be either source and destination, vb replicator will stop or crash on different errors such as not_my_vb as designed, at the old host node, and restart itself at new host node after migration. In particular there would be two scenarios:

##### topology change at source.  
Say, vbucket 123 migrated from node N1 to N2 because this topology change. A new vbucket replicator process (Erlang thread) will start at N2, today we do not replicate checkpoint files to replica within cluster thus the replicator of vb 123 at N2 will start from scratch. It will start from mutation seqNo = 0 and scan all mutations by either sending getMeta to remote side (non-optimistic XDCR), or sending setWithMeta with the doc directly (optimistic XDCR). After scanning all mutations already replicated, the new vb replicator will reach the point to resume replication at N2. In this process, you may see Metadata operations (non-optimistic XDCR) at destination side. If CAPI mode XDCR is used, you will see metadata operations even for optimistic XDCR. Please note that,  in optimistic XMem mode, even you do not see anything on UI at destination. There are a bunch of setWithMeta/delWithMeta sent to the destination. However since they are all rejected by remote ep_engine, we would not see them on UI. But you can query that by looking at ep_engine stats. Namely,

- ep_num_ops_set_meta_res_failed: Number of setWithMeta ops that failed
- ep_num_ops_del_meta_res_failed: Number of delWithMeta ops that failed

##### topology change at destination.
Similar to 1), old replicator will crash because of `not_my_vb` errors returned to source during topology change at destination.  The new vb replicator will be started 30 second later by default, which is configurable. If at this time the topology change at destination is ready and source cluster gets the updated vb map, a new vb replicator will be able to replicate to the new host node at destination cluster.  Again, since we do not have checkpoint files replicated to replica at the destination cluster, the new vb replicator at source need to scan from scratch by sending a bunch of getMeta or setMeta operations to remote node, which is similar to 1).


## Optimizations Under UPR

#### Replicate checkpoint file to intra-cluster replicas
Today we do not replicate any checkpoint file to any intra-cluster replica on source and destination side. As a result, whenever there is a topology change at source or destination making vb replicator restart, the new vb replicator will have to scan from sequence number 0. The reason behind this design is the sequence number at master and replica are not consistent. For example, if you modify a key twice, possibly they do not get deduced at master, resulting two mutations at master but if they get deduced at replica you may see only one mutation.

The arrival of UPR changes that. Now the sequence number is generated at master and replicated to replica, resulting cluster-wise globally consistent sequence number, e.g., seq1000 at master is consistent with seq1000 at any replica. In this case there is obviously room for improvement. We do not need to start scanning from 0 each time we failover to a replica at source, instead, we can resume from the sequence number (checkpoint) stored in checkpoint file after vb replicator restarts at new master.

This requires some infrastructure changes and there could be some performance consequences:

1. replicate the checkpoint files to replicas, apparently there is some overhead, hopefully not much though;
2. checkpointing at master may take longer because it need to wait to ensure the same checkpoint files have been persisted at all local replicas at source cluster;
3. checkpointing may get even longer because it need to ensure not only local replicas commit the checkpoint file, all replicas at remote cluster will also need to be persisted.

This is apparently high overhead optimization with biggest benefit. We can relax it to reduce the overhead, for example, we can choose to only wait for checkpoint files persisted at local replicas, instead of both local and remote. But in the case of destination topology change, we still need to start from scratch since there is no checkpoint file persisted at remote replica.


#### Use remote checkpoint file to restart
Remember today when we do a checkpoint in XDCR, we commit on both sides and thus both remote and source master share the same copy of persisted checkpoint file. When there is any topology change at source cluster and we lose source checkpoint file during migration, we may fetch a duplicate from remote cluster to determine where to restart. However, this assumes the replica and master share globally consistent sequence number and thus this optimization is invalid in pre-3.0 because each node can generates its own sequence number. Introduction of UPR changes that, we will have consistent sequence number across nodes within cluster, enable us to adopt some optimization which is not feasible before.

 
## Benefit Vs. Overhead 
We need to think about the benefit which mostly have been explained versus the overhead of optimization above. For example, if we decide to replicate to checkpoint files to replicas, it is expected that the checkpointing will take much longer than what it takes today, because master need to wait acknowledgement that all checkpoint files have been replicated and persisted in replicas. If we also commits the checkpoint file to the replicas in remote cluster, that may be even longer. Today we do checkpoint every 30 minutes, therefore the overhead is probably tolerable. But in some use cases, where frequent checkpoint is required, we may need to think to make sure if we really want replicating checkpoint files to replicas. Such decision can be made at run-time.



#### more to add here

