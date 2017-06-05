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

#include "collections/vbucket_filter.h"
#include "dcp/stream.h"
#include "tests/mock/mock_dcp_producer.h"

/*
 * Mock of the ActiveStream class. Wraps the real ActiveStream, but exposes
 * normally protected methods publically for test purposes.
 */
class MockActiveStream : public ActiveStream {
public:
    MockActiveStream(EventuallyPersistentEngine* e,
                     mock_dcp_producer_t p,
                     uint32_t flags,
                     uint32_t opaque,
                     const VBucket& vb,
                     uint64_t st_seqno,
                     uint64_t en_seqno,
                     uint64_t vb_uuid,
                     uint64_t snap_start_seqno,
                     uint64_t snap_end_seqno,
                     IncludeValue includeValue = IncludeValue::Yes,
                     IncludeXattrs includeXattrs = IncludeXattrs::Yes)
        : ActiveStream(e,
                       p,
                       p->getName(),
                       flags,
                       opaque,
                       vb.getId(),
                       st_seqno,
                       en_seqno,
                       vb_uuid,
                       snap_start_seqno,
                       snap_end_seqno,
                       includeValue,
                       includeXattrs,
                       std::make_unique<Collections::VB::Filter>(
                               p->getFilter(), vb.getManifest())) {
    }

    // Expose underlying protected ActiveStream methods as public
    void public_getOutstandingItems(VBucketPtr& vb,
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

    bool public_getPendingBackfill() const {
        return pendingBackfill;
    }

    void transitionStateToBackfilling() {
        transitionState(StreamState::Backfilling);
    }

    int getNumBackfillItems() const {
        return backfillItems.memory + backfillItems.disk;
    }

    int getLastReadSeqno() const {
        return lastReadSeqno;
    }

    int getNumBackfillItemsRemaining() const {
        return backfillRemaining;
    }

    std::unique_ptr<DcpResponse> public_makeResponseFromItem(
            queued_item& item) {
        return makeResponseFromItem(item);
    }

    /**
     * Consumes numItems from the stream readyQ
     */
    void consumeBackfillItems(int numItems) {
        std::lock_guard<std::mutex> lh(streamMutex);
        for (int items = 0; items < numItems;) {
            auto resp = backfillPhase(lh);
            if (resp) {
                delete resp;
                ++items;
            }
        }
    }
};

/* Mock of the PassiveStream class. Wraps the real PassiveStream, but exposes
 * normally protected methods publically for test purposes.
 */
class MockPassiveStream : public PassiveStream {
public:
    MockPassiveStream(EventuallyPersistentEngine& e,
                      dcp_consumer_t consumer,
                      const std::string& name,
                      uint32_t flags,
                      uint32_t opaque,
                      uint16_t vb,
                      uint64_t start_seqno,
                      uint64_t end_seqno,
                      uint64_t vb_uuid,
                      uint64_t snap_start_seqno,
                      uint64_t snap_end_seqno,
                      uint64_t vb_high_seqno)
    : PassiveStream(&e,
                    consumer,
                    name,
                    flags,
                    opaque,
                    vb,
                    start_seqno,
                    end_seqno,
                    vb_uuid,
                    snap_start_seqno,
                    snap_end_seqno,
                    vb_high_seqno) {}

    void transitionStateToDead() {
        transitionState(StreamState::Dead);
    }

    ENGINE_ERROR_CODE messageReceived(
            std::unique_ptr<DcpResponse> dcpResponse) override {
        responseMessageSize = dcpResponse->getMessageSize();
        return PassiveStream::messageReceived(std::move(dcpResponse));
    }

    uint32_t responseMessageSize;
};
