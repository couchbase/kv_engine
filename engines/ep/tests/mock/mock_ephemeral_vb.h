/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2017 Couchbase, Inc
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
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
    MockEphemeralVBucket(Vbid i,
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
