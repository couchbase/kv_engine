Read Your Own Write
===================

This spec is about a low latency way to read back a piece of data from a view that a client just stored in the bucket.

Terminology
-----------

### Bucket

A bucket keeps a certain set of items. Views can only index items within the same bucket.

### View

An single index on top of the bucket. It can be a mapreduce or a spatial view.

### View Group

A view group is a collection of views for one bucket. In Couchbase there's one view group for every type of index (mapreduce or spatial) per production design document (development views are a special case that don't matter for this specification).

### Item

An item is a data entry with a key and a value.

### Insert

An insert either creates a new item or an updates an existing one.

### RYOW

Read your own write.

### Sequence Number

See UPR spec.

### Partition

See UPR spec.

### Partition Version

See UPR spec.


Specification
-------------

There are two modes to achieve read your own write. One is stateful, it will have lower overall latency, the other one is stateless. The stateless can only be used for a small number of view groups.

### Client/server architecture

Throughout the spec there's a common architecture of the system, that consists of four parts:

#### Client

In a typical web application the client will be the browser. It interacts through HTTP with the Application.

### Application

The Application is the business logic that runs server-sided and call out to Couchbase. Couchbase itself consists of two parts, the Storage and the Indexer.

### Storage

The storage is the key-value part of Couchbase. It doesn't matter whether the data is persisted on disk. Once it is in the cache, we consider it as "stored".

### Indexer

The Indexer gets its data from the Storage and prepares it for querying. Currently mapreduce and spatial indexers are supported. The result an indexer produces is called view.

The Indexer might be on the same physical machine as the Storage.


### Stateful RYOW

The lower latency of the stateful RYOW comes from the fact that the client is not blocked when inserting an item. The read/write cycle is split into two phases. When the item is stored only the success is returned, a subsequent request will block until it is available in the view group.

The following sequence diagram will explain it in more detail.

    +------+    +-----------+         +-------+  +-------+
    |Client|    |Application|         |Storage|  |Indexer|
    +--+---+    +-----+-----+         +---+---+  +---+---+
       |              |                   |          |
       |1. Insert item|                   |          |
       |------------->|                   |          |
       |              |   2. Insert item  |          |
       |              |------------------>|          |
       |              |                   |          |
       |              |3. seq_num/part_ver|          |
       |              |<------------------|          |
       | 4. Success   |                   |          |
       |<-------------|                              |
       |              |                              |
       |              |                              |
       | 5. Get View  |                              |
       |------------->|                              |
       |              | 6. Get View/seq_num/part_ver |
       |              | (triggers view group update) |
       |              |----------------------------->|
       |              |                              |---+ 7. Block until
       |              |                              |   | seq_num/partition
       |              |                              |   | is indexed by the
       |              |           8. View            |<--+ view group
       |              |<-----------------------------|
       |  9. View     |                              |
       |<-------------|                              |
       |              |                              |


1. Insert item request from the Client to the Application.
2. The Application makes the actual insert request to the Storage
3. The Storage returns the sequence number of the item and the partition version where it is stored.
4. The Application store the sequence number and the partition version of the item and returns a success to the Client. If multiple items are stored, it's only needed to store the highest sequence number for every partition.
5. The Client requests a view from the Application where that just inserted item should be included.
6. The Application knows the items that should be included in the response. The Application then requests from the Indexer the view with the information of the minimal sequence number a partition must have indexed on already.
7. The response is blocked by the Indexer until all partitions have indexed up to the requested sequence number.
8. Once that requirement is fulfilled, the Indexer returns the view to the Application.
9. The Application processes the view as needed and sends the final response back to the client.

#### View group updates

View group updates can not only be triggered by requesting a view, but also by the auto-updater, manually or some external process. This means that such an update can happen between the first Client request (1) and the second one (5). If that update already led the view group to index the new the item, step (7) is a no-op and the view is returned as is.

#### Failures

An item got inserted on a certain node which went done before the Indexer could index it. If a view is requested it will find out that the transmitted UUID from the partition version, doesn't match the index any longer. Hence an error will be returned.

#### Advantages

 - The insert isn't blocked by the indexer. Hence complex indexing is possible without having a slow down on the inserts.
 - A failure on the indexing doesn't have an impact on the data insertion
 - You pay the indexing cost on queries, not the insertion
 - Only the index you query needs to get updated
 - The behaviours described below can be emulated by the client libraries Couchbase provides

#### Disadvantages

 - The application becomes more complex then with the other approaches (though that complexity can easily be hidden by the client libraries Couchbase provides)


### Stateless RYOW

The stateless RYOW blocks the *insert* until it got indexed by *all* view groups. It will ensure that every subsequent request on a View will contain that newly inserted item.

Being indexed by all view groups can take a long time as even indexes that are rarely used need to be updated, as you don't know at insertion time, which view will be queried by the client next. It will be only as fast as the slowest indexer is.

This means that it won't scale with an increasing number of design documents (or larger numbers of indexes per design document).

The following sequence diagram will explain it in more detail.

    +------+    +-----------+         +-------+  +-------+
    |Client|    |Application|         |Storage|  |Indexer|
    +--+---+    +-----+-----+         +---+---+  +---+---+
       |              |                   |          |
       |1. Insert item|                   |          |
       |------------->|                   |          |
       |              |   2. Insert item  |          |
       |              |------------------>|          |
       |              |                   |          |
       |              |3. seq_num/part_ver|          |
       |              |<------------------|          |
       | 4. Store +---|                   |          |
       | seq_num/ |   |                              |
       |partition +-->|     5. Trigger update on     |
       |              |     ALL view groups          |
       |              |----------------------------->|
       |              |                              |---+ 6. Block until
       |              |                              |   | seq_num/partition
       |              |                              |   | is indexed by ALL
       |              |          7. Success          |<--+ view groups
       |              |<-----------------------------|
       |  8. Success  |                              |
       |<-------------|                              |
       |              |                              |
       | 9. Get View  |                              |
       |------------->|                              |
       |              |         10. Get View         |
       |              |----------------------------->|
       |              |           11. View           |
       |              |<-----------------------------|
       |   12. View   |                              |
       |<-------------|                              |
       |              |                              |


1. Insert item request from the Client to the Application.
2. The Application makes the actual insert request to the Storage
3. The Storage returns the sequence number of the item and the partition version where it is stored. If multiple items are stored, it's only needed to store the highest sequence number for every partition.
4. The Application store the sequence number and the partition version of the item.
5. The Application then triggers an update of *all* view groups with the information of the minimal sequence number a partition must have indexed on already.
6. The response is blocked by the Indexer until all partitions of all view groups have indexed up to the requested sequence number.
7. Once that requirement is fulfilled, the Indexer returns success to the Application.
8. The Application returns a success to the Client.
9. The Client requests any view from the Application.
10. The Application requests the view from the Indexer
11. The indexer immediately response with the view, as it is ensured that the newly inserted item was already indexed.
12. The Application processes the view as needed and sends the final response back to the client.

#### Advantages

 - Querying the index return immediately

#### Disadvantages

 - Failures in the Storage and Indexer get connected
 - A failure on the indexing needs to make the insert fail, although the insert itself might have succeeded
 - The insertion blocks until the Indexer caught up. That means slower insertion rates
 - All indexes need to be updated, i.e. rarely used indexes slow down insertions. For example if you have user facing indexes and analitical ones, that don't need to be indexed immediately (or are complex and slow). The insertion would block until the the analytical indexes are updated as well, as you don't know which index will be queried next.


Blocking RYOW
-------------

The application doesn't need any code, the server blocks the inserts until all index are updated. This is similar to the consistency that ACID compliant databases guarantee.

    +------+    +-----------+      +-------+            +-------+
    |Client|    |Application|      |Storage|            |Indexer|
    +--+---+    +-----+-----+      +---+---+            +---+---+
       |              |                |                    |
       |1. Insert item|                |                    |
       |------------->|                |                    |
       |              | 2. Insert item |                    |
       |              |--------------->|                    |
       |              |                |                    |
       |              |            +---|                    |
       |              |3. seq_num/ |   |                    |
       |              |   part_ver |   |                    |
       |              |            +-->|4. Trigger update on|
       |              |                |  ALL view groups   |
       |              |                |------------------->|
       |              |                |                    |---+ 5. Block until
       |              |                |                    |   | seq_num/partition
       |              |                |                    |   | is indexed by ALL
       |              |                |     6. Success     |<--+ view groups
       |              |                |<-------------------|
       |              |   7. Success   |                    |
       |              |<---------------|                    |
       |  8. Success  |                |                    |
       |<-------------|                                     |
       |              |                                     |
       | 9. Get View  |                                     |
       |------------->|                                     |
       |              |            10. Get View             |
       |              |------------------------------------>|
       |              |              11. View               |
       |              |<------------------------------------|
       |   12. View   |                                     |
       |<-------------|                                     |
       |              |                                     |

1. Insert item request from the Client to the Application.
2. The Application makes the actual insert request to the Storage
3. The Storage keeps the sequence number of the item and the partition version where it is stored. If multiple items are stored, it's only needed to store the highest sequence number for every partition.
4. The storage then triggers an update of *all* view groups with the information of the minimal sequence number a partition must have indexed on already.
5. The response is blocked by the Indexer until all partitions of all view groups have indexed up to the requested sequence number.
6. Once that requirement is fulfilled, the Indexer returns success to the Storage.
7. The Storage returns success to the Application.
8. The Application returns a success to the Client.
9. The Client requests any view from the Application.
10. The Application requests the view from the Indexer.
11. The indexer immediately response with the view, as it is ensured that the newly inserted item was already indexed.
12. The Application processes the view as needed and sends the final response back to the client.

#### Advantages

 - The same advantages as the stateless RYOW
 - The Application code becomes simpler

#### Disadvantages

 - All disadvantages from the stateless RYOW
 - The storage on the server side needs to care about indexers (i.e. more complexity on the server sided code)

