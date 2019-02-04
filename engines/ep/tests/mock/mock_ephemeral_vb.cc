#include "config.h"

#include "mock_ephemeral_vb.h"

#include "ephemeral_tombstone_purger.h"
#include "failover-table.h"

MockEphemeralVBucket::MockEphemeralVBucket(
        Vbid i,
        vbucket_state_t newState,
        EPStats& st,
        CheckpointConfig& chkConfig,
        KVShard* kvshard,
        int64_t lastSeqno,
        uint64_t lastSnapStart,
        uint64_t lastSnapEnd,
        std::unique_ptr<FailoverTable> table,
        NewSeqnoCallback newSeqnoCb,
        SyncWriteCompleteCallback syncWriteCb,
        Configuration& config,
        item_eviction_policy_t evictionPolicy)
    : EphemeralVBucket(i,
                       newState,
                       st,
                       chkConfig,
                       kvshard,
                       lastSeqno,
                       lastSnapStart,
                       lastSnapEnd,
                       std::move(table),
                       std::move(newSeqnoCb),
                       syncWriteCb,
                       config,
                       evictionPolicy,
                       std::make_unique<Collections::VB::Manifest>()) {
    /* we want MockBasicLinkedList instead to call certain non-public
       APIs of BasicLinkedList in ephemeral vbucket */
    this->seqList = std::make_unique<MockBasicLinkedList>(st);
    mockLL = dynamic_cast<MockBasicLinkedList*>((this->seqList).get());
}

size_t MockEphemeralVBucket::markOldTombstonesStale(rel_time_t purgeAge) {
    // Mark all deleted items in the HashTable which can be purged as Stale -
    // this removes them from the HashTable, transferring ownership to
    // SequenceList.

    HTTombstonePurger purger(purgeAge);
    purger.setCurrentVBucket(*this);
    ht.visit(purger);

    return purger.getNumItemsMarkedStale();
}
