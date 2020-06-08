/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2017-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

/*
 * Mock of the BasicLinkedList class.  Wraps the real BasicLinkedList class
 * and provides access to functions like getAllItemsForVerification().
 */
#pragma once

#include "../mock/mock_basic_ll.h"
#include "ephemeral_vb.h"

#include <mutex>
#include <vector>

class MockEphemeralVBucket : public EphemeralVBucket {
public:
    MockEphemeralVBucket(
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
            SyncWriteTimeoutHandlerFactory syncWriteNextExpiryChangedFact,
            SeqnoAckCallback seqnoAckCb,
            CheckpointDisposer ckptDisposer,
            Configuration& config,
            EvictionPolicy evictionPolicy,
            std::unique_ptr<Collections::VB::Manifest> manifest);

    /* Register fake shared range lock for testing */
    RangeGuard registerFakeSharedRangeLock(seqno_t start, seqno_t end) {
        return mockLL->registerFakeSharedRangeLock(start, end);
    }

    /* Register fake exclusive range lock for testing */
    RangeGuard registerFakeRangeLock(seqno_t start, seqno_t end) {
        return mockLL->registerFakeRangeLock(start, end);
    }

    int public_getNumStaleItems() {
        return mockLL->getNumStaleItems();
    }

    int public_getNumListItems() {
        return mockLL->getNumItems();
    }

    int public_getNumListDeletedItems() {
        return mockLL->getNumDeletedItems();
    }

    uint64_t public_getListHighSeqno() const {
        return mockLL->getHighSeqno();
    }

    MockBasicLinkedList* getLL() {
        return mockLL;
    }

    /**
     * Convenience method to run the HT tombstone purger across the entire
     * vBucket (this is normally done with a seperate task using the
     * pause/resumevisitor).
     * @return Count of items marked stale and moved to sequenceList.
     */
    size_t markOldTombstonesStale(rel_time_t purgeAge);

    void public_doCollectionsStats(
            const Collections::VB::CachingReadHandle& cHandle,
            const VBNotifyCtx& notifyCtx);

private:
    /* non owning ptr to the linkedlist in the ephemeral vbucket obj */
    MockBasicLinkedList* mockLL;
};
