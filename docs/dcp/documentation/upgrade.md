
# Upgrade (2.x to 3.x)

Couchbase 3.x contains both the TAP and DCP protocols. As a result the cluster manager can choose to use either of the protocols for replication. In order to make things as simple as possible when upgrading the cluster manager only uses the TAP protocol when in a mixed cluster scenario (eg. during the upgrade). Once all nodes are running Couchbase 3.x the cluster manager will start closing TAP streams and replacing those closed streams with DCP streams. The cluster manager will continue to do this until the entire cluster is using the DCP protocol for replication.

Switching from a TAP stream to a DCP stream will cause replica VBuckets to be rematerialized because the DCP protocol requires that each replica VBucket has the exact same mutation history as the active VBucket. As a result the cluster manager will slowly switch TAP streams to DCP streams in order to avoid too many replicas being completely rematerialized at the same time. To ensure this the cluster manager limits the amount of streams switched on individual nodes to one incoming stream and one outgoing stream. Slowly switching streams reduces the chance of dataloss in the case of node failures.

Once the upgrade is complete all nodes will replicate data using DCP.
