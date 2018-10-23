# Terminology

The purpose of this document is to provide definitions for common terms that are used throughout the DCP documentation.

**DCP Connection:** A TCP network connection over which the DCP Protocol is used. 

**DCP Stream:** A sequence of data mutations for a given VBucket.

**Consumer:** The endpoint in a DCP connection that recieves data.

**History Branch:** Occurs whenever a VBucket changes its state from non-active to active state or in the event of a failover or uncontrolled shutdown and restart. Since sequence numbers are assigned only by the active VBucket it is possible that during failures that sequence numbers might be reassigned. When this happens we say that the mutation history has branched. To resolve branching conflicts we add a new failover log entry to the active VBucket in cases where sequence numbers might be reassigned (eg. in failure scenarios).

**Failover Log:** A log of all possible history branches. Always contains one or more failover log entries.

**Failover Log Entry:** A VBucket UUID and sequence number pair associated with a VBucket. A failover log entry is assigned to an active VBucket any time there might have been a history branch. The VBucket UUID is a randomly generated number used to denote a history branch and the sequence number is the last sequence number processed by the VBucket at the time the failover log entry was created.

**Mutation:** An event that deletes a key or changes the value a key points to. Mutations occur when transactions such as create, update, delete or expire are executed.

**Producer:** The endpoint in a DCP connection that sends data.

**Rollback:** Due to a history branch a Consumer might need to remove some of its data so that it can be sure that it has the same data as the Producer it connected to. A rollback occurs when the Producer asks the Consumer to remove all of its data received after a specific sequence number.

**Sequence Number:** Each mutation that occurs on a vBucket is assigned a number, which strictly increases as events are assigned numbers (there is no harm in skipping numbers, but they must increase), that can be used to order that event against other mutations within the same VBucket. This does not give a cluster-wide ordering of events, but it does enable processes watching events on a vBucket to resume where they left off after a disconnect.

**Snapshot:** A set of unique ordered keys that is sent by a DCP Stream. When a full snapshot is received the Consumer is guarenteed to have a consistent view of the database up to the last sequence number recieved in the snapshot.

**VBucket:** A computed subset of all possible keys in Couchbase (eg. shard or parition).

**VBucket UUID:** A randomly generated number which is used to denote a history branch.


