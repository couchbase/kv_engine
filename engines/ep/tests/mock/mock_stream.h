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
                     std::shared_ptr<MockDcpProducer> p,
                     uint32_t flags,
                     uint32_t opaque,
                     VBucket& vb,
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
                       vb,
                       st_seqno,
                       en_seqno,
                       vb_uuid,
                       snap_start_seqno,
                       snap_end_seqno,
                       includeValue,
                       includeXattrs,

                       p->getFilter(),
                       vb.getManifest()) {
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

    const std::queue<std::unique_ptr<DcpResponse>>& public_readyQ() {
        return readyQ;
    }

    std::unique_ptr<DcpResponse> public_nextQueuedItem() {
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

    void transitionStateToInMemory() {
        transitionState(StreamState::InMemory);
    }

    void transitionStateToTakeoverSend() {
        transitionState(StreamState::TakeoverSend);
    }

    void transitionStateToTakeoverWait() {
        transitionState(StreamState::TakeoverWait);
    }

    void transitionStateToTakeoverDead() {
        transitionState(StreamState::Dead);
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
                ++items;
            }
        }
    }

    bool public_handleSlowStream() {
        return handleSlowStream();
    }

    void setState(StreamState state) {
        state_ = state;
    }
};

/**
  * Variation of the MockActiveStream class, which overloads the registerCursor
  * method.  In addition it implements two additional methods
  * (continueRegisterCursor and waitForRegisterCursor), which are used to
  * control the when registerCursor is executed.
  */
class MockActiveStreamWithOverloadedRegisterCursor : public MockActiveStream {
public:
    MockActiveStreamWithOverloadedRegisterCursor(
            EventuallyPersistentEngine* e,
            std::shared_ptr<MockDcpProducer> p,
            uint32_t flags,
            uint32_t opaque,
            VBucket& vb,
            uint64_t st_seqno,
            uint64_t en_seqno,
            uint64_t vb_uuid,
            uint64_t snap_start_seqno,
            uint64_t snap_end_seqno,
            IncludeValue includeValue = IncludeValue::Yes,
            IncludeXattrs includeXattrs = IncludeXattrs::Yes)
        : MockActiveStream(e,
                           p,
                           flags,
                           opaque,
                           vb,
                           st_seqno,
                           en_seqno,
                           vb_uuid,
                           snap_start_seqno,
                           snap_end_seqno,
                           includeValue,
                           includeXattrs) {
    }

    /**
      * Overload of the ActiveStream registerCursor method.  It first executes
      * the callback function which is used to inject additional work prior to
      * the execution of the method ActiveStream::registerCursor.
      */
    virtual void registerCursor(CheckpointManager& chkptmgr,
                                uint64_t lastProcessedSeqno) override {
        callbackBeforeRegisterCursor();
        ActiveStream::registerCursor(chkptmgr, lastProcessedSeqno);
        callbackAfterRegisterCursor();
    }

    // Function that sets the callback function.  The callback is invoked at the
    // start of the overloaded registerCursor method.
    void setCallbackBeforeRegisterCursor(std::function<void()> func) {
        callbackBeforeRegisterCursor = func;
    }

    // Function that sets the callback function.  The callback is invoked at the
    // end of the overloaded registerCursor method.
    void setCallbackAfterRegisterCursor(std::function<void()> func) {
        callbackAfterRegisterCursor = func;
    }

    /**
      * The callback function which is used to perform additional work on its
      * first invocation.  The function moves checkpoints forward, whilst in the
      * middle of performing a backfill.
      */
    std::function<void()> callbackBeforeRegisterCursor;

    // The callback function which is used to check the state of
    // pendingBackfill after the call to ActiveStream::registerCursor.
    std::function<void()> callbackAfterRegisterCursor;
};

/* Mock of the PassiveStream class. Wraps the real PassiveStream, but exposes
 * normally protected methods publically for test purposes.
 */
class MockPassiveStream : public PassiveStream {
public:
    MockPassiveStream(EventuallyPersistentEngine& e,
                      std::shared_ptr<DcpConsumer> consumer,
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
                        vb_high_seqno) {
    }

    void transitionStateToDead() {
        transitionState(StreamState::Dead);
    }

    ENGINE_ERROR_CODE messageReceived(
            std::unique_ptr<DcpResponse> dcpResponse) override {
        responseMessageSize = dcpResponse->getMessageSize();
        return PassiveStream::messageReceived(std::move(dcpResponse));
    }

    size_t getNumBufferItems() const {
        LockHolder lh(buffer.bufMutex);
        return buffer.messages.size();
    }

    uint32_t responseMessageSize;
};
