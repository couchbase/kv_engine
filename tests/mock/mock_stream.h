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

#pragma once

#include "dcp/stream.h"

/*
 * Mock of the ActiveStream class. Wraps the real ActiveStream, but exposes
 * normally protected methods publically for test purposes.
 */
class MockActiveStream : public ActiveStream {
public:
    MockActiveStream(EventuallyPersistentEngine* e,
                     dcp_producer_t p,
                     const std::string& name,
                     uint32_t flags,
                     uint32_t opaque,
                     uint16_t vb,
                     uint64_t st_seqno,
                     uint64_t en_seqno,
                     uint64_t vb_uuid,
                     uint64_t snap_start_seqno,
                     uint64_t snap_end_seqno)
        : ActiveStream(e,
                       p,
                       name,
                       flags,
                       opaque,
                       vb,
                       st_seqno,
                       en_seqno,
                       vb_uuid,
                       snap_start_seqno,
                       snap_end_seqno) {
    }

    // Expose underlying protected ActiveStream methods as public
    void public_getOutstandingItems(RCPtr<VBucket>& vb,
                                    std::vector<queued_item>& items) {
        getOutstandingItems(vb, items);
    }

    void public_processItems(std::vector<queued_item>& items) {
        processItems(items);
    }

    bool public_nextCheckpointItem() {
        return nextCheckpointItem();
    }

    const std::queue<DcpResponse*>& public_readyQ() {
        return readyQ;
    }

    DcpResponse* public_nextQueuedItem() {
        return nextQueuedItem();
    }

    void public_setBackfillTaskRunning(bool b) {
        isBackfillTaskRunning = b;
    }

    bool public_isBackfillTaskRunning() const {
        return isBackfillTaskRunning;
    }

    void public_transitionState(stream_state_t newState) {
        transitionState(newState);
    }

    bool public_getPendingBackfill() const {
        return pendingBackfill;
    }
};
