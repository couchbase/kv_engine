# kvlite

kvlite (ns_lite would probably be a better name) is a minimal implementation
of a couchbase server with kv only. It allows a "standard" way to initalize
a cluster and create buckets, but unlike ns_server it does not monitor
and take actions on the individual nodes if it detects that something
isn't what it would expect it be.

# Why?

So why do we have this? We already had the code to do most of this in
our "cluster_framework" that we use in the automated clustered tests,
but sometimes one would like to experiment with a system and I figured
I could just reuse the parts we had and put it behind the same rest
interface we've got so that I could utilize the same "couchbase-cli"
commands we already know to configure the system.

Once the cluster is up'n'running I may now experiment with the
cluster of memcached processes and toggle the vbuckets in the
way I want without having ns_server reset them back to how they're
supposed to work. Another bonus is that one may experiment with
stuff ns_server lacks support for (during development) without
having to hack other parts of the system.

# How

## Start kvlite

Currently kvlite creates a temporary directory in the current directory
named `cluster_XXXXXX` where `XXXXXXX` is a unique number. It is
currently not possible to start kvlite again and pick up an existing
database etc.

By default kvlite listens to port 6666 (currently only IPv4 as it
looks like our REST interface can only deal with either IPv4 or IPv6),
but the port number may be changed by using the `--port` parameter:

    $ ./kvlite --port 1234

## Initialize cluster

Initialize the cluster with

    couchbase-cli cluster-init --cluster http://127.0.0.1:6666 \
                               --cluster-username Administrator \
                               --cluster-password asdfasdf \
                               --cluster-port 6666 \
                               --cluster-ramsize 1024 \
                               --services data

Most of the parameters is currently ignored (the command needs to be
executed in for the other commands to work). The only parameters used
is the username and password. 

This will start up a 4 node cluster on the local machine.

## Create a bucket

Create your bucket with

    couchbase-cli bucket-create \
                     --cluster http://127.0.0.1:6666 \
                     -u Administrator \
                     -p asdfasdf \
                     --bucket mybucket \
                     --bucket-type couchbase \
                     --storage-backend couchstore \
                     --durability-min-level none \
                     --bucket-ramsize 200 \
                     --bucket-replica 1 \
                     --bucket-priority low \
                     --bucket-eviction-policy fullEviction \
                     --enable-flush 0 \
                     --enable-index-replica 0 \
                     --compression-mode off

You should be able to fetch the terse bucket map with

    curl http://127.0.0.1:6666/pools/default/b/mybucket

## Connect your client

The only known port in kvlite is the cluster port, so you need
to fetch the terse bucket map to find data ports:

     curl http://127.0.0.1:6666/pools/default/b/mybucket | jq .nodesExt
     
And you should be able to bootstrap your client over CCCP.

The system also defines another administrator user (it is used by the
underlying test framework):  `@admin` with password `password`

# whats next

* store the current configuration and allow for restart
* Add/remove (local) nodes dynamically
* Add/remove (remote) nodes dynamically
* add support for moving vbuckets (rebalance)
