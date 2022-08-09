/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2016-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#pragma once

#include "dcp/active_stream.h"
#include "dcp/backfill.h"
#include "dcp/producer.h"
#include "dcp/stream.h"

class ActiveStreamCheckpointProcessorTask;
struct BackfillScanBuffer;
class MockActiveStream;
class MockBucketLogger;
class MockDcpMessageProducers;

/*
 * Mock of the DcpProducer class.  Wraps the real DcpProducer, but exposes
 * normally protected methods publically for test purposes.
 */
class MockDcpProducer : public DcpProducer {
public:
    MockDcpProducer(EventuallyPersistentEngine& theEngine,
                    const CookieIface* cookie,
                    const std::string& name,
                    uint32_t flags,
                    bool startTask = true);

    using DcpProducer::updateStreamsMap;

    cb::engine_errc maybeDisconnect() {
        return DcpProducer::maybeDisconnect();
    }

    cb::engine_errc maybeSendNoop(DcpMessageProducersIface& producers) {
        return DcpProducer::maybeSendNoop(producers);
    }

    void setLastReceiveTime(const rel_time_t timeValue) {
        lastReceiveTime = timeValue;
    }

    void setNoopSendTime(const rel_time_t timeValue) {
        noopCtx.sendTime = timeValue;
    }

    rel_time_t getNoopSendTime() {
        return noopCtx.sendTime;
    }

    bool getNoopPendingRecv() {
        return noopCtx.pendingRecv;
    }

    void setNoopEnabled(const bool booleanValue) {
        noopCtx.enabled = booleanValue;
    }

    bool getNoopEnabled() {
        return noopCtx.enabled;
    }

    void setDCPExpiry(bool value) {
        enableExpiryOpcode = value;
        if (enableExpiryOpcode) {
            // Expiry opcode uses the same encoding as deleteV2 (includes
            // delete time); therefore a client enabling expiry_opcode also
            // implicitly enables includeDeletetime.
            includeDeleteTime = IncludeDeleteTime::Yes;
        }
    }

    bool getDCPExpiry() const {
        return enableExpiryOpcode;
    }

    void setSyncReplication(SyncReplication value) {
        supportsSyncReplication = value;
    }

    /**
     * Create the ActiveStreamCheckpointProcessorTask and assign to
     * checkpointCreator->task
     */
    void createCheckpointProcessorTask() {
        DcpProducer::createCheckpointProcessorTask();
    }

    /**
     * Schedule the checkpointCreator->task on the ExecutorPool
     */
    void scheduleCheckpointProcessorTask() {
        DcpProducer::scheduleCheckpointProcessorTask();
    }

    ActiveStreamCheckpointProcessorTask* getCheckpointSnapshotTask() const;

    /**
     * Finds the stream for a given vbucket
     */
    std::shared_ptr<ActiveStream> findStream(Vbid vbid);

    /**
     * Finds the stream for a given vbucket/sid
     * @returns a pair where second indicates if the VB has no entries at all
     *          first is the stream (or null if no stream)
     */
    std::pair<std::shared_ptr<ActiveStream>, bool> findStream(
            Vbid vbid, cb::mcbp::DcpStreamId sid);

    /**
     * Sets the backfill buffer size (max limit) to a particular value
     */
    void setBackfillBufferSize(size_t newSize);

    void setBackfillBufferBytesRead(size_t newSize);

    bool getBackfillBufferFullStatus();

    /*
     * @return A reference to BackfillManager::scanBuffer
     */
    BackfillScanBuffer& public_getBackfillScanBuffer();

    UniqueDCPBackfillPtr public_dequeueNextBackfill();

    BackfillManager& getBFM() {
        return *backfillMgr;
    }

    BackfillManager* getBFMPtr() {
        return backfillMgr.get();
    }

    size_t getBytesOutstanding() const {
        return log.getBytesOutstanding();
    }

    void ackBytesOutstanding(size_t bytes) {
        return log.acknowledge(bytes);
    }

    VBReadyQueue& getReadyQueue() {
        return ready;
    }

    /**
     * Place a mock active stream into the producer
     */
    std::shared_ptr<MockActiveStream> mockActiveStreamRequest(
            uint32_t flags,
            uint32_t opaque,
            VBucket& vb,
            uint64_t start_seqno,
            uint64_t end_seqno,
            uint64_t vbucket_uuid,
            uint64_t snap_start_seqno,
            uint64_t snap_end_seqno,
            IncludeValue includeValue = IncludeValue::Yes,
            IncludeXattrs includeXattrs = IncludeXattrs::Yes,
            IncludeDeletedUserXattrs includeDeleteUserXattrs =
                    IncludeDeletedUserXattrs::No,
            std::optional<std::string_view> jsonFilter = {},
            std::function<void()> preSetActiveHook = {});

    /**
     * Step the producer and expect the opcode to be returned
     */
    cb::engine_errc stepAndExpect(MockDcpMessageProducers& producers,
                                  cb::mcbp::ClientOpcode expectedOpcode);

    /**
     * Call step(), but wrap the producers with a DcpMsgProducersBorderGuard (
     * in the same way it is called from EvPEngine::step().
     *
     * Useful when operating directly on a (Mock)DcpProducer object without
     * ep_engine, but need to ensure currentEngine switching is still correct.
     */
    cb::engine_errc stepWithBorderGuard(DcpMessageProducersIface& producers);

    void enableMultipleStreamRequests() {
        multipleStreamRequests = MultipleStreamRequests::Yes;
    }

    void enableStreamEndOnClientStreamClose() {
        sendStreamEndOnClientStreamClose = true;
    }

    bool scheduleBackfillManager(VBucket& vb,
                                 std::shared_ptr<ActiveStream> s,
                                 uint64_t start,
                                 uint64_t end) override {
        beforeScheduleBackfillCB(end);
        return DcpProducer::scheduleBackfillManager(
                vb, std::move(s), start, end);
    }

    void setBeforeScheduleBackfillCB(std::function<void(uint64_t)> backfillCB) {
        beforeScheduleBackfillCB = backfillCB;
    }

    std::function<void(uint64_t)> beforeScheduleBackfillCB = [](uint64_t) {};

    void setCloseAllStreamsHook(std::function<void()> hook) {
        closeAllStreamsHook = hook;
    }

    void setCloseAllStreamsPostLockHook(std::function<void()> hook) {
        closeAllStreamsPostLockHook = hook;
    }

    void setCloseAllStreamsPreLockHook(std::function<void()> hook) {
        closeAllStreamsPreLockHook = hook;
    }

    void setSeqnoAckHook(std::function<void()> hook) {
        seqnoAckHook = hook;
    }

    IncludeValue public_getIncludeValue() const {
        return includeValue;
    }

    IncludeXattrs public_getIncludeXattrs() const {
        return includeXattrs;
    }

    IncludeDeletedUserXattrs public_getIncludeDeletedUserXattrs() const {
        return includeDeletedUserXattrs;
    }

    // sets the value used to generate the totalBytes stat
    void setTotalBtyesSent(size_t v) {
        totalBytesSent = v;
    }

    void setupMockLogger();

    MockBucketLogger& public_getLogger() const;

    void setOutOfOrderSnapshots(OutOfOrderSnapshots oso);
};
