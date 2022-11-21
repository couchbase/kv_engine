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

#pragma once

#include "dcp/active_stream.h"
#include "dcp/passive_stream.h"

class CheckpointManager;
class MockDcpProducer;

/*
 * Mock of the ActiveStream class. Wraps the real ActiveStream, but exposes
 * normally protected methods publically for test purposes.
 */
class MockActiveStream : public ActiveStream {
public:
    MockActiveStream(
            EventuallyPersistentEngine* e,
            std::shared_ptr<MockDcpProducer> p,
            uint32_t flags,
            uint32_t opaque,
            VBucket& vb,
            uint64_t st_seqno = std::numeric_limits<uint64_t>::min(),
            uint64_t en_seqno = std::numeric_limits<uint64_t>::max(),
            uint64_t vb_uuid = 0,
            uint64_t snap_start_seqno = std::numeric_limits<uint64_t>::min(),
            uint64_t snap_end_seqno = std::numeric_limits<uint64_t>::max(),
            IncludeValue includeValue = IncludeValue::Yes,
            IncludeXattrs includeXattrs = IncludeXattrs::Yes,
            IncludeDeletedUserXattrs includeDeletedUserXattrs =
                    IncludeDeletedUserXattrs::No,
            std::optional<std::string_view> jsonFilter = {});

    // Expose underlying protected ActiveStream methods as public
    OutstandingItemsResult public_getOutstandingItems(VBucket& vb) {
        return getOutstandingItems(vb);
    }

    void public_processItems(OutstandingItemsResult& result) {
        std::lock_guard<std::mutex> lh(streamMutex);
        processItems(result, lh);
    }

    bool public_nextCheckpointItem(DcpProducer& producer) {
        return nextCheckpointItem(producer);
    }

    const std::queue<std::unique_ptr<DcpResponse>>& public_readyQ() {
        return readyQ;
    }

    size_t public_readyQSize() {
        std::lock_guard<std::mutex> lh(streamMutex);
        return readyQ.size();
    }

    std::unique_ptr<DcpResponse> public_nextQueuedItem(DcpProducer& producer);

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

    void transitionStateToDead() {
        transitionState(StreamState::Dead);
    }

    int getNumBackfillItems() const {
        return backfillItems.memory + backfillItems.disk;
    }

    uint64_t getLastReadSeqno() const {
        return lastReadSeqno;
    }

    std::optional<cb::NonNegativeCounter<size_t>> getNumBackfillItemsRemaining()
            const {
        return backfillRemaining;
    }

    std::unique_ptr<DcpResponse> public_makeResponseFromItem(
            queued_item& item,
            SendCommitSyncWriteAs sendMutationInsteadOfCommit);

    /**
     * Consumes numItems from the stream readyQ
     */
    void consumeBackfillItems(DcpProducer& producer, int numItems);

    /// Consumes all ready backfill items.
    void consumeAllBackfillItems(DcpProducer& producer);

    bool public_handleSlowStream() {
        return handleSlowStream();
    }

    void setState(StreamState state) {
        state_ = state;
    }

    OutstandingItemsResult getOutstandingItems(VBucket& vb) override {
        preGetOutstandingItemsCallback();
        return ActiveStream::getOutstandingItems(vb);
    }

    /// A callback to allow tests to inject code before we access the checkpoint
    std::function<void()> preGetOutstandingItemsCallback = [] { return; };

    void public_registerCursor(CheckpointManager& manager,
                               const std::string& name,
                               int64_t seqno);

    bool isDead() { return ActiveStream::getState() == StreamState::Dead; };

    std::unique_ptr<DcpResponse> public_popFromReadyQ();

    bool public_supportSyncReplication() const {
        return supportSyncReplication();
    }

    cb::mcbp::DcpStreamId getStreamId() const {
        return sid;
    }

    void setCompleteBackfillHook(std::function<void()> hook) {
        completeBackfillHook = hook;
    }

    void setNextHook(std::function<void(const DcpResponse*)> hook) {
        nextHook = hook;
    }

    void setTakeoverSendPhaseHook(std::function<void()> hook) {
        takeoverSendPhaseHook = hook;
    }

    uint64_t getLastBackfilledSeqno() const {
        std::lock_guard<std::mutex> lh(streamMutex);
        return lastBackfilledSeqno;
    }

    IncludeValue public_getIncludeValue() const {
        return includeValue;
    }

    IncludeXattrs public_getIncludeXattrs() const {
        return includeXattributes;
    }

    IncludeDeletedUserXattrs public_getIncludeDeletedUserXattrs() const {
        return includeDeletedUserXattrs;
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
    void registerCursor(CheckpointManager& chkptmgr,
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
                      Vbid vb,
                      uint64_t start_seqno,
                      uint64_t end_seqno,
                      uint64_t vb_uuid,
                      uint64_t snap_start_seqno,
                      uint64_t snap_end_seqno,
                      uint64_t vb_high_seqno,
                      const Collections::ManifestUid vb_manifest_uid)
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
                        vb_high_seqno,
                        vb_manifest_uid) {
    }

    void transitionStateToDead() {
        transitionState(StreamState::Dead);
    }

    cb::engine_errc messageReceived(std::unique_ptr<DcpResponse> dcpResponse);

    void processMarker(SnapshotMarker* marker) override {
        PassiveStream::processMarker(marker);
    }

    cb::engine_errc processMutation(
            MutationConsumerMessage* mutation) override {
        return PassiveStream::processMutation(mutation);
    }

    auto& getBufferMessages() const {
        return buffer.messages;
    }

    void setProcessBufferedMessages_postFront_Hook(
            std::function<void()>& hook) {
        processBufferedMessages_postFront_Hook = hook;
    }

    std::unique_ptr<DcpResponse> public_popFromReadyQ();

    const std::queue<std::unique_ptr<DcpResponse>>& public_readyQ() const {
        return readyQ;
    }

    const std::string public_createStreamReqValue() const {
        return createStreamReqValue();
    }

    bool getCurSnapshotPrepare() const {
        return cur_snapshot_prepare.load(std::memory_order_relaxed);
    }
};
