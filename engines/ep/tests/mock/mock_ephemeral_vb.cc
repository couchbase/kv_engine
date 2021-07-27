/*
 *     Copyright 2021-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#include "mock_ephemeral_vb.h"
#include "collections/vbucket_manifest.h"
#include "ephemeral_tombstone_purger.h"
#include "failover-table.h"
#include "kvstore/kvstore.h"

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
        SyncWriteResolvedCallback syncWriteResolvedCb,
        SyncWriteCompleteCallback syncWriteCb,
        SeqnoAckCallback seqnoAckCb,
        Configuration& config,
        EvictionPolicy evictionPolicy,
        std::unique_ptr<Collections::VB::Manifest> manifest)
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
                       syncWriteResolvedCb,
                       syncWriteCb,
                       seqnoAckCb,
                       config,
                       evictionPolicy,
                       std::move(manifest)) {
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

void MockEphemeralVBucket::public_doCollectionsStats(
        const Collections::VB::CachingReadHandle& cHandle,
        const VBNotifyCtx& notifyCtx) {
    VBucket::doCollectionsStats(cHandle, notifyCtx);
}
