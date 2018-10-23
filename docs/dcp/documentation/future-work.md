## Future Work

#### Connection Filters

We should be able to filter documents in order to allow a consumer to specify a stream where only documents matching a specific pattern are sent. Filters can be either a regex or some sort of plugable code mechanism. We should be able to filter on keys, values, meta data, and operation type.

#### Slow Connection Detection (Tentative for 3.1)

We need to be able to deal with connections that do not read data quickly. Since we don't allow items in an in-memory checkpoint to be purged from memory if a cursor is registered a slow connection can cause high memory overhead in the server. One proposal for this is to drop the cursor from memory and close the stream forcing the client to reconnect the stream once it is ready to receive more items.

#### Connection Prioritization

We need a way to specify that certain DCP connections should be visited and receive a higher priority than other connections. This will allow us to make sure replication is always getting the most attention, followed by xdcr and indexing, followed by backup/restore and other integration plugins.

#### Synchronous Replication

We need to implement some sort of event notification system that will tell the producer when certain events have occurred on the consumer. These events for example could be item recieved (eg. replicated) and item persisted.
