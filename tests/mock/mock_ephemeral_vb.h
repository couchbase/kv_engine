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
#include "config.h"
#include "ephemeral_vb.h"

#include <mutex>
#include <vector>

class MockEphemeralVBucket : public EphemeralVBucket {
public:
    MockEphemeralVBucket(id_type i,
                         vbucket_state_t newState,
                         EPStats& st,
                         CheckpointConfig& chkConfig,
                         KVShard* kvshard,
                         int64_t lastSeqno,
                         uint64_t lastSnapStart,
                         uint64_t lastSnapEnd,
                         std::unique_ptr<FailoverTable> table,
                         NewSeqnoCallback newSeqnoCb,
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
                           config,
                           evictionPolicy) {
        /* we want MockBasicLinkedList instead to call certain non-public
           APIs of BasicLinkedList in ephemeral vbucket */
        this->seqList = std::make_unique<MockBasicLinkedList>(st);
        mockLL = dynamic_cast<MockBasicLinkedList*>((this->seqList).get());
    }

    /* Register fake read range for testing */
    void registerFakeReadRange(seqno_t start, seqno_t end) {
        mockLL->registerFakeReadRange(start, end);
    }

    int public_getNumStaleItems() {
        return mockLL->getNumStaleItems();
    }

    int public_getNumListItems() {
        return mockLL->getNumItems();
    }

private:
    /* non owning ptr to the linkedlist in the ephemeral vbucket obj */
    MockBasicLinkedList* mockLL;
};
