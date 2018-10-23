Unified Protocol for Replication Design Specification
=================================================

# INTRODUCTION

## Purpose of this Document

This document describes the software design and implementation to support the Unified Protocol for Replication (UPR) inside Couchbase Server as both a client and server, and a generic client implementation.

## Motivation

UPR is a mechanism for efficiently streaming mutations from a Master partition server to replicas/slaves, indexers (Incremental Map/Reduce, GeoSpatial, Elastic Search), XDCR, incremental backup, and third party systems.

It's intention is to allow consistency of data and work efficiently and reliably with the Couchbase networked distributed system and all problems associated, including network slowness and disconnects, slow servers, slow clients, server rebalance/partition relocation, server failover, server crashes and data loss, client crashes and data loss.

## Scope of this Document

This document will describe the high level algorithms, data structures, and components necessary to implement UPR. It will not specify byte level protocol implementation details, or required changes in existing code necessary to support the implementation.

# SYSTEM ARCHITECTURAL DESIGN

## Overview of Modules and Components

### Couchbase Server

This is a single node of a Couchbase installation. It contains an implementation of all components.

### memcached/ep-engine

This is the networked data serving component of Couchbase Server.

### management layer/ns_server

ns_server is the server component responsible for cluster management and deciding which servers are master, replicas and when to rebalance and failover.

### Couchstore

This is the storage engine component of Couchbase Server. It arranges a record of each document by update sequence ID, a  monotonically increasing value, in a btree. Each document mutation is a assigned a new sequence number, for existing documents in the store, the old sequence value is deleted. This allows servers to quickly scan recent documents and deletions persisted since a particular sequence number, or from zero if wants all changes.

### Write Queue/Immutable Tree

These are per-partition, in-memory immutable tree ordered by update seq that allows for snapshotting unpersisted items by multiple concurrent consumers (clients and flusher), allowing each an independent snapshot while also allowing concurrent updates. The immutable tree will allow for relatively low memory consumption, fast reads and writes, and automatic freeing of unreachable tree nodes and values.

The tree nodes link to immutable data and are ref counted, with each node pointed to by one or more parent snapshots, and the root ref count is incremented by each partition write queue manager. 

Creating a snapshot is simply incrementing the root once and handing it off to the snapshot user. When all users of that root/snapshot are done, including the write queue manager, the ref count of the root falls to zero and the node is freed, and any child nodes/values it points have their ref count decremented, and if they fall to zero, are also freed. This happens recursively to immediately free any unreferenced subtrees and values.

Possible library we can use, from the LLVM project:

<http://llvm.org/docs/ProgrammersManual.html#llvm-adt-immutableset-h>

<http://llvm.org/docs/doxygen/html/ImmutableSet_8h.html>

### Immutable Values

Values that are assigned to the bucket/partition hashtable are immutable and ref counted. The hash table record itself is not immutable and can be assigned a new immutable values.

When the value is retrieved from the hashtable, the hash table record is locked, and the value ref count is incremented, and then the record is unlocked. When the value is no longer referenced by the hash table, write queue or any other process/thread in the server, it is automatically freed as it's ref count falls to zero.

### Failover Log

The failover log tracks a history of failover events so that UPR clients can find a safe sequence number to stream a new partition snapshot with a guarantee of not missing any changes. A failover event is generated when a master is started and it's unknown if the master was completely in sync with all mutations from the previous master. When that happens, a new failover ID is generated and failover sequence of the last successful snapshot.

If the master starts up but doesn't know if a clean shutdown occurred, a new failover log entry is generated, even if there there is no failover to a new machine. Semantically this is the same as failing over to a new machine, because we don't know if someone else has seen changes that we never persisted.

The failover log is a list of UUIDs/sequence pairs and each is marker of high sequence number of last complete snapshot the new master persisted from the old master. If a new log entry has a sequence number lower than existing entries, those existing entries are removed from the log.

Clients can wait to persist the failover log whenever it completely persists a snapshot, as well as the high sequence snapshot it most recently persisted.

When a client connects to a server and before it stream a new partition snapshot, it will compare it's failover log with the server to find the highest safe sequence number it can stream.

### Client and Mutation Log

The client is a process that can talk to a server and streaming changes from it. It will have a mutation log of the latest key mutations to provide a way to rollback changes if it is ahead the of a server after a failover or crash. The maximum log size should be configurable, so the oldest entries are removed when it exceeds the length. If the client needs to use a sequence that's lower than the latest sequence it's seen, it will use the mutation log to undo any changes that might be out of sync with the master before streaming from the common sequence number.

### Barrier Cookies

Barrier cookies are a way a for a indexer to ensure consistency with storage quickly and at multiple points in time, so that UPR indexer clients can service multiple concurrent query clients demanding consistent results. UPR clients generate unique IDs and send them to the server, and the server sends them back once all mutations since that point in time have been streamed to the client.

# NETWORK INTERFACE DESIGN AND FLOW

## How KV mutations flow into ep-engine and write queue
1. A mutation (insert,update,delete) request from an memcached client occurs.
2. The hash table entry where the item will reside is locked. This might be granular to the entry itself, or something higher like the whole hashtable.
3. If a CAS operation, the mutation is either accepted or rejected.
4. If the mutation is accepted, the partition seq is incremented and assigned to the mutation.
5. The mutation is placed into the locked hash table entry with a new seq item, and then locks the write queue.
6. If an existing item, the old seq from the hashtable entry is used to delete the previous by seq entry from the write queue by being marked as a seq key to delete.
7. If a deleted item, the item in the hash table is kept around, and will be until the deletion is persisted.
8. The hash table partition is unlocked.
9. The client gets a response that the item was accepted.

## How KV mutations flow into Replicas

When a server owns a replica partition, there is a UPR client feeding it mutations. This can be inside of ep-engine, or an external client feeding it the mutations in order. Once in the replica server, the flow is the same as a master server, with the exception that the mutations are already assigned the sequence update # by the master, and the replica preserves that sequence.

It is also important that the replica persist the mutation in sequence order. This is already guaranteed as long as items are sent in sequence order, and the replica persists everything in the partition write queue in a single commit.

## How front end clients ensure mutations are persisted to disk on master

Clients when performing a mutation can either block until the mutation is persisted, or receive the sequence update number, partition ID and current server failover ID and waits until the server has persisted it.

There can be 3 kinds of operations of the front end client to ensure persistence:

1. The server blocks on the mutation response until the mutation is durable, by monitoring internally when the persisted sequence number is greater than or equal to assigned sequence number.
2. The server sends back the high update sequence #, and the client makes a new type of connection, sends the sequence # and partition ID and failover ID, and the server blocks until persistence has completed through that sequence.
3. The server sends back the high update sequence #, and the client polls the server with the sequence #, partition ID and failover ID, and keeps polling until the server has completed persistence through that sequence.

For 2 and 3 above, if the server's failover ID doesn't match the ID supplied by the client, it returns an error. The client will need to reexamine the server's version of the document and possibly re-perform the mutation to ensure it's complete. If the server is no longer the master of that partition, it will return a "not my partition" error and the client will reload the cluster map and retry.

## How front end clients ensure mutations are replicated or persisted on a replica

There can be 2 kinds of operations of the front end client to ensure replication or replica persistence. First the client gets the new sequence for a mutation transaction from the server, then:

2. The master server sends back the high update sequence #, and the client makes a new type of connection to a replica, sends the sequence # and partition ID and failover ID, and the replica server blocks until the replica has received a snapshot higher or equal to the sequence # returned by the master server.
3. The server sends back the high update sequence #, and the client polls the server with the sequence #, partition ID and failover ID, and keeps polling until the server has completed snapshot persistence through that sequence.

For 2 and 3 above, if the server's failover ID doesn't match the ID supplied by the client, it returns an error. The client will need to reexamine the server's version of the document and possibly re-perform the mutation to ensure it's complete. If the server is no longer the master of that partition, it will return a "not my partition" error and the client will reload the cluster map and retry.

Note: because of subsequent updates by another client and reduplication of item, the client can't just wait for the sequence to arrive at the replica, since it might have been elided by subsequent updates. Waiting for doc id is more expensive than necessary, instead we wait for a complete snapshot from the master where the high snapshot sequence # is equal to or higher than supplied by the client.

## How we can get single document ACID

**This is outside the scope of the current implementation.**

Assuming we'll never have multiset transactions (which would require distributed transactions), the thing preventing single document ACID is the lack of Isolation of a single mutation. Mutations in this scheme will still be visible by all other clients before they are durable or replicated, which, if there is master failure, means updates can be seen by clients and then subsequently lost.

Future versions of Couchbase will likely provide this capability.

## How items in the partition write queue flow to couchstore

1. The flusher thread locks the write queue and increments the counter on the root node, giving it a snapshot of the partition, then unlocks the queue.
2. The flusher then walks the tree and copies the metadata and data into parallel arrays and hands them to Couchstore.
3. Once the all the items are successfully persisted, the queue is then locked and all items persisted are deleted from the tree.
4. Any deletions persisted are then removed from the hashtable, if the items in the hashtable still have the same sequence number and haven't been mutated again.


## How UPR clients and ep-engine handshake

1. An UPR client makes a connection.
2. The UPR client sends the partition numbers for the partitions it wants to stream, the allowable states (active, replica, dead, pending), it also sends an identifier for what and who it is in the form of "%what%:%who%". The identifier is so the server can track stats and differentiate between replica, indexer, xdcr etc in it's own stat tracking and reporting.
3. ep-engine then sends the failover log for each partition it owns.
4. For each partition it wishes to stream, the client sends to the server partition number. expected state of active or replica, and latest failover ID and start seq number.
5. For any partition the server doesn't own, is in the wrong state, or has too high a seq # or the wrong current failover ID, it sends a error to the client. The client is expected to disconnect, reload the client map and start back on step 1.

## How ep-engine sends snapshots to UPR clients

6. The server will send a complete snapshot of each partition, one at a time. It will send the snapshot of each partition with mutations in partition ID order.
6. For each partition with mutations. The server will first send a partition start message, which includes the partition ID it will send, then stream all changes, in sequence order with the sequence number. The server may send duplicates for items, but it will never omit an item.
7. If a partition has no mutations, the server will send no partition start message, it will skip it.
7. When it's checked or sent all partitions, the server will send a "completed all partitions message".
7. The server then loops around and streams any new mutations for the registered partitions. If there are no mutations, the server will pause until there are mutations, or until it gets barrier cookie from the client.

## Barrier cookies, how clients request faster/multiple complete snapshots

8. If the client, who is also a secondary index server, wishes to get a faster consistent view at a point in time, or multiple consistent snapshots for it's own clients, it sends to the server a "barrier cookie", a short text string that is unique per UPR connection.
9. The server then notes the new barrier cookie and keeps any old barrier cookie(s) around that is hasn't yet sent.
9. When the barrier cookie order is received and all partitions have been streamed after the barrier cookie is added, it sends back the barrier cooke so the client knows it's seen a consistent snapshot for all partitions.
10. The server forgets a barrier after sending it back.
12. When the client no longer wishes to stream changes, it breaks the connection.
13. If a partition the client is streaming becomes inactive or replica, the server returns a not "not my partition error" and the client reloads the partition map and re-handshakes.

## How ep-engine streams a consistent snapshot for a partition

1. Each partition has it's own in-memory write queue, which is an immutable tree ordered by sequence ID.
2. For each UPR connection, ep-engine maintains a list of high seq numbers for each partition.
2. When sending a stream for a partition, it grabs the root of the tree and increments the ref counter for the root. This prevents the root for being freed if it's concurrently updated.
2. It notes the high seq number for that snapshot.
3. If the starting seq number is on disk, not memory, it walks the Couchstore update sequence btree.
4. For any non-deleted item with an in-memory update seq that's lower than high seq number, it grabs it out of memory if resident, otherwise it loads it from disk.
5. It sends each item's metadata and body to the client.
6. Once it's finished walking the disk btree, it then walks the in-memory by seq tree, sending each items metadata and body to the client.

## How ep-engine tracks barrier cookies

1. For each UPR connection, ep-engine maintains a fifo queue of barrier markers and number of times it's cycled through all the partition.
2. When ep-engine gets a barrier cookie, it records the number of cycles it's completed and the partition it's currently streaming. This find the point after where it should send back the barrier cookie to the client. It places this into the barrier queue. 
3. Every time ep-engine completes a cycle for all partitions, it increments the cycle counter.
4. After each partition snapshot is completed or skipped it checks the barrier queue to see if it's streamed every partition since the barrier was added. It does this by checking if the number of cycles and the next partition are greater than the barrier marker. If so, it sends the barrier cookie back to the client and pops it out of the queue.
5. If there are not more mutations to stream, it sends back all barrier cookie state with the snapshot high seq.


## How clients roll back changes when seeing a new failover ID

1. Each client maintains a ring buffer of recent mutations, and the last complete snapshot seq number it processed for each partition as well as the failover log from the master. The ring buffer should be persisted before mutations are applied to persistence, unless the client also has the ability to natively rollback changes. 
2. A client requests list of failover logs from the server for each partition.
3. For each partition it determine the rollback sequence/start stream sequence by comparing it's failover log and snapshot sequence with the failover log from the master.
4. It rolls back it's own changes to the rollback sequence number, by requesting the latest document for each higher seq in its ring buffer, and inserting it/processing it. If the document doesn't exist, it deletes the document from it's state.
5. If it is a replica overwriting our own entries, we need to preserve the sequence number from the master in our storage as we do this. This means the ordering of the live documents might no longer be ordered on disk by sequence, but they will still be properly ordered in the by_sequence btree.
5. If the document on the master server hasn't been modified since the rollback sequence number and it is a replica, it can optionally "repair" the master by sending the document to the master, letting it generate a new sequence and then purging our own entry. We'll receive repaired documents again when we stream the snapshot.
5. If needs to rollback to a sequence that's lower than it's ring buffer contents, it sets the starting sequence number to 0.
5. It removes the higher seq item IDs from the ring buffer.
6. It asks the server for a snapshot of changes using the start sequence number, and applies them to the ring buffer and it's state.
7. When done with the snapshot, it records the servers failover log in it's state and the snapshot high seq.

## How UPR Replicas/Clients find out where to rollback to and stream from

When an UPR client handshakes with the master, it needs to determine the rollback sequence number to unwind it's potentially out of sync state, which is also the same start snapshot sequence number it will then stream from the master.

The algorithm below determines that sequence number for any state the master and client/replica can be in.

1. A replica will request the failover log from the current master to compare with it's own failover log.
2. If not equal, the replica will add a dummy entry with sentinel Id to the front of it's own failover log, with the current high persisted snapshot sequence (if it never persisted a snapshot sequence number, it uses 0). Any earlier entries with a seq greater than the current high persisted will be removed.
3. It will add a dummy entry with sentinel Id to the front of the log for the master, with the highest sequence the replica has seen, but not necessarily a snapshot.
4. If there is a common ancestor in the log, grab next newer entry from both logs compare the sequences.
5. If both Failover IDs are sentinels, take the largest of the two as the rollback sequence. If not, the smaller of the two is the rollback sequence number.
6. If there is no common ancestor, 0 is the rollback sequence.

## How ns_server performs a partition rebalance

1. ns_server initiates a pending partition on a new node, or selects an existing replica and puts it into the pending state.
2. If a new replica, an UPR client (either inside ep-engine or external) connects to the master and begins streaming.
3. Once the replica is caught up the master, or nearly caught up (as decided by the algorithm or heuristic inside ns_server), ns_server tells the master to put it's partition into the dead state, so that it no longer accepts new mutations. ns_server can monitor how caught up a replica is by asking the master and replica for the current high seq it's seen and/or persisted.
4. Once all mutations are sent to the replica and persisted without error, (ns_server will monitor this in the same way a client ensures persistence), then ns_server tells the new server to set it's partition state to active, and then tells the master to either delete it's partition or put it into the replica state.
5. If there is an fatal error or timeout in step 4, ns_server will tell the master to go back into the active state. If not possible, it will perform a failover to another replica.


## How ns_server performs failover

If ns_server detects the master is crashed or unresponsive, it can put a replica in the cluster into the active state and assign it a new failover ID in a single operation. It can then initiate a new replica as an UPR client of the master.

## How ns_server and ep-engine guarantee ordering of partition state transitions

There is a rare, but possible, problem of mis-ordering of partition transition changes and other partition states.

If ns_server attempts to set a partition to another state, but loses the connection with ep-engine before getting back a success response, it's possible that it will send another state transition command which will be processed out of order in ep-engine, putting the partition in the incorrect state.

This is because ep-engine can still receive the first command after the second, because of non-deterministic multi-threading and the possibility of the message from the broken connection still residing in network buffers.

A simple fix is to use a CAS mechanism when modifying partition state. There will be a single CAS for a bucket.

1. On startup, ep-engine will generate a CAS and start partitions in the previous state persisted, with front-end client connections turned off.
2. ns_server will poll to wait until ep-engine has warmed up all partitions, and to discover the CAS.
3. ns_server will send any new state/commands using the CAS, to set appropriate state and turn on front-end client connections.
4. If the CAS matches and the command and states are accepted, ep-engine will respond with success and new CAS, which will be noted by ns_server.
5. If the CAS doesn't match or the command has an error, ep-engine will return the appropriate error.
6. If a CAS error, ep_engine will return an error with the correct CAS and ns_server will record the correct CAS and retry.
7. If another error, ns_server will log and possibly take another action, like a retry.

## How UPR stats are tracked

For each UPR connection, the identifying string in the client handshake is concatenated with the partition ID and all stats for that connection are suffixed with that string. Replica's UPR connections will identify destination node with the string "Replica:%NodeID%" where node ID is the identifier of the replica. The concatenated stat identifier will look like "%StatName%:%PartitionID%:Replica:%NodeId%".

ns_server will retrieve all stats for UPR connections, and will parse the stats to discover if they are from a replica, vs a view or backup, etc, by parsing the stat name.

## How to maintain backwards compatibility with existing TAP replicas/masters.

Comments from Trond:

From a TAP point of view it shouldn't be hard to make it backwards compatible, because the "old" tap just contains two different methods (with some mutations like in the vbucket move that the last thing it does is to mark the bucket as dead and disconnect the client etc).: 

* Start give me live mutations
* Send me everything you got and keep sending me stuff.

It is the _client_ that dictates the method to use (and this is instantiated by ns_server, so it knows the versions being used on both ends and may start the transfer with the appropriate flag). 

In the first message sent to the server we need to add a new TAP FLAG indicating that it want to use UPR (in future versions we may want to disconnect clients who don't set this flag when we no longer want to support the old one). We may then either use the "engine-specific" parts of the "old" tap messages, or use the TAP_OPAQUE message type to transfer additional information.

# Examples


## Finding RollbackSeq/StartSeq from the failover logs

Before we compare we add some special values to the tables to make the algorithm work:

`ffffffff` - Server sentinel, always highest seq of client #

`00000000` - Client sentinel, always client last snapshot seq

###First connect, Replica has no log, no snapshot:
<table>
<tr>
	<th colspan=2>Master Log</th><th colspan=2>Client Log</th>
</tr>
<tr>
	<th>Failover</th><th>At</th>
	<th>Failover</th><th>At</th>
</tr>
<tr>
	<td><code>ffffffff</code></td><td>0</td>
	<td><code>00000000</code></td><td>0</td>
</tr>
<tr>
	<td><code>cafebabe</code></td><td>0</td>
	<td></td><td></td>
</tr>
</table>

No common ancestor:

**RollbackSeq/StartSeq is 0**

###No failover, no snapshot, seen through seq 5:
<table>
<tr>
	<th colspan=2>Master Log</th><th colspan=2>Client Log</th>
</tr>
<tr>
	<th>Failover</th><th>At</th>
	<th>Failover</th><th>At</th>
</tr>
<tr>
	<td><code>ffffffff</code></td><td>5</td>
	<td><code>00000000</code></td><td>0</td>
</tr>
<tr>
	<td><code>cafebabe</code></td><td>0</td></td>
	<td><code>cafebabe</code></td><td>0</td></td>
</tr>
</table>

`cafebabe` common ancestor, the next row are both sentinels, take the larger of the two:

**RollbackSeq/StartSeq is 5**

###No failover, snapshot at 6, seen through seq 7:
<table>
<tr>
	<th colspan=2>Master Log</th><th colspan=2>Client Log</th>
</tr>
<tr>
	<th>Failover</th><th>At</th>
	<th>Failover</th><th>At</th>
</tr>
<tr>
	<td><code>ffffffff</code></td><td>7</td>
	<td><code>00000000</code></td><td>6</td>
</tr>
<tr>
	<td><code>cafebabe</code></td><td>0</td></td>
	<td><code>cafebabe</code></td><td>0</td></td>
</tr>
</table>

`cafebabe` common ancestor, the next row are both sentinels, take the larger of the two:

**RollbackSeq/StartSeq is 7**

###`deadbeef` master failover occurred at 5, snapshot at 6, seen through seq 7:
<table>
<tr>
	<th colspan=2>Master Log</th><th colspan=2>Client Log</th>
</tr>
<tr>
	<th>Failover</th><th>At</th>
	<th>Failover</th><th>At</th>
</tr>
<tr>
	<td><code>ffffffff</code></td><td>7</td>
	<td></td><td></td>
</tr>
<tr>
	<td><code>deadbeef</code></td><td>5</td></td>
	<td><code>00000000</code></td><td>6</td></td>
</tr>
<tr>
	<td><code>cafebabe</code></td><td>0</td></td>
	<td><code>cafebabe</code></td><td>0</td></td>
</tr>
</table>

`cafebabe` common ancestor, take smaller of next higher row seq:

**RollbackSeq/StartSeq is 5**

###`deadbeef` master failover occurred at 8, snapshot at 6, seen through seq 7:
<table>
<tr>
	<th colspan=2>Master Log</th><th colspan=2>Client Log</th>
</tr>
<tr>
	<th>Failover</th><th>At</th>
	<th>Failover</th><th>At</th>
</tr>
<tr>
	<td><code>ffffffff</code></td><td>7</td>
	<td></td><td></td>
</tr>
<tr>
	<td><code>deadbeef</code></td><td>8</td></td>
	<td><code>00000000</code></td><td>6</td></td>
</tr>
<tr>
	<td><code>cafebabe</code></td><td>0</td></td>
	<td><code>cafebabe</code></td><td>0</td></td>
</tr>
</table>

`cafebabe` common ancestor, take smaller of next higher row seq:

**RollbackSeq/StartSeq is 6**

###`deadbeef` master failover occurred at 8, client last saw master failover at `ba5eba11` at 7, snapshot at 7, seen through seq 9:
<table>
<tr>
	<th colspan=2>Master Log</th><th colspan=2>Client Log</th>
</tr>
<tr>
	<th>Failover</th><th>At</th>
	<th>Failover</th><th>At</th>
</tr>
<tr>
	<td><code>ffffffff</code></td><td>9</td>
	<td><code>00000000</code></td><td>7</td>
</tr>
<tr>
	<td><code>deadbeef</code></td><td>8</td></td>
	<td><code> ba5eba11 </code></td><td>7</td></td>
</tr>
<tr>
	<td><code>cafebabe</code></td><td>0</td></td>
	<td><code>cafebabe</code></td><td>0</td></td>
</tr>
</table>

`cafebabe` common ancestor, take smaller of next higher row seq:

**RollbackSeq/StartSeq is 7**

###`deadbeef` master failover occurred at 8, client last saw master failover at `ba5eba11` at 7, snapshot at 6, seen through seq 9:
<table>
<tr>
	<th colspan=2>Master Log</th><th colspan=2>Client Log</th>
</tr>
<tr>
	<th>Failover</th><th>At</th>
	<th>Failover</th><th>At</th>
</tr>
<tr>
	<td><code>ffffffff</code></td><td>9</td>
	<td><code>00000000</code></td><td>6</td>
</tr>
<tr>
	<td><code>deadbeef</code></td><td>8</td></td>
	<td><code> ba5eba11 </code></td><td>7</td></td>
</tr>
<tr>
	<td><code>cafebabe</code></td><td>0</td></td>
	<td><code>cafebabe</code></td><td>0</td></td>
</tr>
</table>

Because the last snapshot we saw, `00000000` at 6, is less than the failover entry `ba5eba11` at 7, we elide the `ba5eba11` entry.
<table>
<tr>
	<th colspan=2>Master Log</th><th colspan=2>Client Log</th>
</tr>
<tr>
	<tr><th>Failover</th><th>At</th>
	<th>Failover</th><th>At</th>
</tr>
<tr>
	<td><code>ffffffff</code></td><td>9</td>
	<td></td><td></td>
</tr>
<tr>
	<td><code>deadbeef</code></td><td>8</td></td>
	<td><code>00000000</code></td><td>6</td></td>
</tr>
<tr>
	<td><code>cafebabe</code></td><td>0</td></td>
	<td><code>cafebabe</code></td><td>0</td></td>
</tr>
</table>

`cafebabe` common ancestor, take smaller of next higher row seq:

**RollbackSeq/StartSeq is 6**



###`deadbeef` master created when no replica's were available, client last saw master failover at `ba5eba11` at 7, snapshot at 7, seen through seq 9:
<table>
<tr>
	<th colspan=2>Master Log</th><th colspan=2>Client Log</th>
</tr>
<tr>
	<th>Failover</th><th>At</th>
	<th>Failover</th><th>At</th>
</tr>
<tr>
	<td><code>ffffffff</code></td><td>9</td>
	<td><code>00000000</code></td><td>7</td>
</tr>
<tr>
	<td><code>deadbeef</code></td><td>0</td></td>
	<td><code> ba5eba11 </code></td><td>7</td></td>
</tr>
<tr>
	<td></td><td></td></td>
	<td><code>cafebabe</code></td><td>0</td></td>
</tr>
</table>

No common ancestor, must rollback everything:

**RollbackSeq/StartSeq is 0**

### No failover of master, client last saw master failover at `ba5eba11` at 7, snapshot at 7, seen through seq 9:
<table>
<tr>
	<th colspan=2>Master Log</th><th colspan=2>Client Log</th>
</tr>
<tr>
	<th>Failover</th><th>At</th>
	<th>Failover</th><th>At</th>
</tr>
<tr>
	<td></td><td></td>
	<td><code>00000000</code></td><td>7</td>
</tr>
<tr>
	<td><code>ffffffff</code></td><td>9</td></td>
	<td><code>ba5eba11</code></td><td>7</td></td>
</tr>
<tr>
	<td><code>cafebabe</code></td><td>0</td></td>
	<td><code>cafebabe</code></td><td>0</td></td>
</tr>
</table>

**Not possible!** If the replica saw a master with the failover ID after `cafebabe`, it's impossible for the current master to still be at `cafebabe`, since it would have had to not be master, then become master and generate a new failover ID.


## UPR in action on a Cluster

Here is a three node cluster with replica failovers and new nodes being added.

First we have server A as the master of partition 1. In the real world, Server A will be master of many partitions and most of the operations here will be happening with multiple partitions.

The clients in the example are replicas, so they don't need a ringbuffer to rollback since they already have a built-in log of changes with the by_sequence btree.

### Initialization

When the master comes online, it generates a failover GUID and indicates that it came active at seq 0.

There is already a client load on the master.

The replicas pull the master log and apply it to it's storage.

![Figure 1](images/failover-1.png)

### Begin streaming mutations

Master server A has set into it's memory 5 mutations, which it has not yet persisted.

Replica B starts streaming from sequence 0. The master snapshots the unpersisted mutations through seq 5, and starts sending them to the replica.

![Figure 2](images/failover-2.png)

### Continue streaming mutations

Master server A has set 3 more mutations, it has already persisted all mutations to durable storage.

Replica B is still streaming from sequence 0 to 5, it has already persisted through sequence 4.

Replica C starts streaming from sequence 0. The master snapshots the mutations and starts sending them to the replica.

![Figure 3](images/failover-3.png)

### Continue streaming mutations, next snapshot

Master server A has set 3 more mutations, it has already persisted all mutations to durable storage.

Replica B has finished streaming the snapshot and persisted all mutations. It persists its last snapshot sequence at 5. Master starts streaming the next snapshot for mutations 6 to 8.

Replica C has finished streaming the snapshot and persisted all mutations. It persists it's last snapshot sequence at 8.

![Figure 4](images/failover-4.png)

### Master goes down

Master server A dies/becomes unresponsive. It's removed from the cluster.

![Figure 5](images/failover-5.png)

### Replica becomes Master

Replica B becomes Master server B. It generates a new entry in the failover log, noting the high sequence of the last complete snapshot it persisted.

![Figure 6](images/failover-6.png)

### Replicas recognize new master

Already the new master has started accepting mutations.

New Replica D gets the log and saves it.

Existing Replica C connects to new master and gets the failover log to compare with it's own.

![Figure 7](images/failover-7.png)

### Replica C repairs master and rolls back to common snapshot sequence

Replica D starts streaming from the master.

Replica C discovers it has mutations possibly not on the master. It will repair the master for documents that haven't been updated since the rollback point, the repaired documents will get a new sequence on the master, the replica then purges those documents from it's storage.

It applies to its storage the masters version of any documents it couldn't repair, preserving all metadata and sequence numbers. It then persists it's new last snapshot number and the failover log into storage.

![Figure 8](images/failover-8.png)

### Streaming mutations from new master

Both server D and server A continue to stream in mutations, continuing operations as normal.

![Figure 9](images/failover-9.png)
