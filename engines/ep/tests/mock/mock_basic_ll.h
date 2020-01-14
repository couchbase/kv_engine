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

#include "linked_list.h"

#include <mutex>
#include <vector>

class MockBasicLinkedList : public BasicLinkedList {
public:
    MockBasicLinkedList(EPStats& st) : BasicLinkedList(Vbid(0), st) {
    }

    OrderedLL& getSeqList() {
        return seqList;
    }

    std::vector<seqno_t> getAllSeqnoForVerification() const {
        std::vector<seqno_t> allSeqnos;
        std::lock_guard<std::mutex> lckGd(writeLock);

        for (auto& val : seqList) {
            allSeqnos.push_back(val.getBySeqno());
        }
        return allSeqnos;
    }

    /// Expose the rangeReadLock for testing.
    std::mutex& getRangeReadLock() {
        return rangeReadLock;
    }

    /* Register fake read range for testing */
    void registerFakeReadRange(seqno_t start, seqno_t end) {
        (*readRange.lock()) = SeqRange(start, end);
    }

    void resetReadRange() {
        readRange.lock()->reset();
    }
};
