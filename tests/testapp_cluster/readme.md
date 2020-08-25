# Cluster test

The cluster test framework allows you to create a cluster of 'n' nodes on the
same machine to test cross-node functionality (replication, durability etc).

## Quick howto

    auto cluster = cb::test::createCluster(4);
    auto bucket = cluster->createBucket("test", {{"replicas", 3},
                                                 {"max_size", 67108864}});

At this point you've got a cluster with a bucket named test and all of
the replication streams should be up'n'running. The cluster provides
a way to get a connection to a given node, or to get a connection to
the node where a given vbucket lives (with the specified state).
Note that you would need to authenticate and select bucket (the system
can't select the bucket for you as that would need to happen after
an authentication phase):

    auto connection = bucket->getConnection(Vbid{0});
    connection->authenticate("foo", "bar", "PLAIN");
    connection->selectBucket("test");
    conn->store("foo", Vbid(0), "value");

## Limitations

* Can't move vbuckets around (rebalance)
