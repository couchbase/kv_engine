## Rollback

Rollback is necessary when a DCP consumer has a different history from a DCP producer.
Rollback happens at the vbucket level. When a stream is created for a vbucket it is decided whether a consumer needs a rollback or not. The rollback point (the sequence number to which the consumer needs to rollback) is decided with the help of the [failover logs](failure-scenarios.md) which are kept for all VBuckets no matter what state the VBucket might be in. Essentially the Consumer removes all of its data after the rollback point.

In this document we discuss about the rollback logic. DCP [protocol flow](protocol-flow.md) involving  rollback is a separate document.

Before we can understand rollback logic well, we need to know the following terms:

1. **Snapshot**: A set of unique ordered keys that is sent by a DCP Stream. When a full snapshot is received the Consumer is guarenteed to have a consistent view of the database up to the last sequence number recieved in the snapshot. If consumer does not have full snapshot, then the items in the snapshot are not necessarily consistent due to de-duplication.
2. **SnapStartSeqno**: Start sequence number of a snapshot.
3. **SnapEndSeqno**: End sequence number of a snapshot.
4. **StartSeqno**: Sequence number from which consumer requests data.
5. **EndSeqno**: Sequence number until which consumer requests data.
6. **VBucket UUID**: A randomly generated 64-bit value which is used to denote a history branch.
7. **PurgeSeqno**: Max sequence number of a purged item from the storage at the Producer.

When a "stream request" is made by a consumer, it contains snapStartSeqno, snapEndSeqno, startSeqno, endSeqno and the latest vbucket_uuid in its failover table. A correct request will have the below invariant

						SnapStartSeqno <= StartSeqno <= SnapEndSeqno

### Rollback logic on DCP Producer

DCP producer uses the snapStartSeqno, snapEndSeqno, startSeqno, endSeqno and the latest vbucket_uuid in the consumer request and purge seqno and its own failover table to decide the rollback point. The producer **adjusts the SnapStartSeqno and SnapEndSeqno** to calculate the rollback point more effectively. If StartSeqno is equal to SnapEndSeqno, then the consumer has the entire snapshot and hence producer sets SnapStartSeqno to SnapEndSeqno in its rollback calculations. If StartSeqno is equal to SnapStartSeqno, then the consumer no items in the snapshot and hence producer sets SnapEndSeqno to SnapStartSeqno in its rollback calculations.

Once the producer adjusts the SnapStartSeqno and SnapEndSeqno, the rollback logic can be explained in below cases.

#### 1. Base Case
#### a. Consumer has no history at seqno 0
	StartSeqno == 0 and Consumer VBucket UUID == 0
**Rollback is not necessary** as 0 is the smallest sequence number we have and the consumer has passed a uuid indicating that it is ok to receive items irrespective of the history @ seqno 0 in the producer failover table.

#### b. Consumer has a history at seqno 0
	StartSeqno == 0 and Consumer VBucket UUID != 0
**Rollback is requested** even though 0 is the smallest sequence number we have. This is because a consumer might need to update its state correctly when it has a diverging history even though at seqno 0. We go through sections (3) and (4) below to determine if the consumer has a history mismatch with the producer or not. 

#### 2. Wild Card 'Purge'
	SnapStartSeqno < PurgeSeqno and StartSeqno != 0
The consumer needs to **full rollback to 0** (if the requested start seqno is not already 0). This is necessary because for a consistent view the consumer should not miss out on any deleted (and subsequently purged items) on the producer.

#### 3. Diverging History
	Consumer VBucket UUID not found in producer's failover table
Producer and consumer have no common histories. Hence **full rollback to 0**.

#### 4. Partial/Full History
Consumer VBucket UUID match found in producer's failover table
When a VBucket UUID match is found then that means the producer and consumer have same history till the corresponding seqno associated with that uuid. The next point in the producer history is the next seqno in the producer's failover table if there is one, otherwise it is simply the high seqno on the producer. Let's denote this as **upper**.
#### a. Full history match
	Lagging Consumer (SnapEndSeqno <= upper)
Consumer **need not rollback** here as it has same history as producer and can simple resume from where it left off the last time.

#### b. Partial history match
	Leading consumer (SnapStartSeqno > upper)
Here consumer has a snapshot that is not present in the producer. Hence the producer will ask the consumer to **partial rollback to upper**. The consumer has to rollback till that point and resend the stream request with its snapshot info at the rollback point (upper in this case). The producer then runs its rollback logic again to see if the consumer needs further rollback.

#### c. Partial history match
	Consumer Snapshot mismatch (SnapStartSeqno <= upper < SnapEndSeqno)
This indicates that a consumer has a snapshot that is not present in the producer. Hence the consumer will have to **partial rollback to SnapStartSeqno** and post another stream request. Producer then will send items from SnapStartSeqno in a new snapshot.

### Rollback logic on DCP Consumer
"Stream request" is made by a consumer which it contains snapStartSeqno, snapEndSeqno, startSeqno, endSeqno and the latest vbucket_uuid in its failover table.
Data received by a DCP consumer is preceeded by a **snapshot marker**. It contains SnapStartSeqno and SnapEndSeqno for the data that is going to be received by the consumer in that snapshot. The consumer should store those values, so that in case of a partial rollback, it can resend the stream request with the appropriate SnapStartSeqno and SnapEndSeqno at the rollback point.

In case of a rollback, the producer sends its failover log and the rollback point. The consumer should replace its failover log with the one sent by the producer and resend the stream request with the appropriate SnapStartSeqno and SnapEndSeqno at the rollback point. 

