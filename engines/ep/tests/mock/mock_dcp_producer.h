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
                    CookieIface* cookie,
                    const std::string& name,
                    cb::mcbp::DcpOpenFlag flags,
                    bool startTask = true);

    using DcpProducer::updateStreamsMap;

    cb::engine_errc maybeDisconnect() {
        return DcpProducer::maybeDisconnect();
    }

    cb::engine_errc maybeSendNoop(DcpMessageProducersIface& producers) {
        return DcpProducer::maybeSendNoop(producers);
    }

    void setNoopSendTime(std::chrono::steady_clock::time_point timeValue) {
        noopCtx.sendTime = timeValue;
    }

    std::chrono::steady_clock::time_point getNoopSendTime() {
        return noopCtx.sendTime;
    }

    bool getNoopPendingRecv() {
        return noopCtx.pendingRecv;
    }

    enum class NoopMode {
        /// Noops disabled. Prevents enabling XATTRs or collections (as they
        /// both require noops to be on to configured).
        Disabled,
        /// Noops enabled, will be sent at the default interval
        /// (DcpProducer::defaultDcpNoopTxInterval) unless otherwise specified.
        Enabled,
        /// Noops enabled, but the TX interval is set to an arbitrarily large
        /// value, so in practical terms will never be sent.
        /// Unless a test actually *requires* Noops, this mode is preferred
        /// over 'Enabled'.
        /// The purpose of this setting is to "enable" NOOPs - which in turn
        /// allows newer features like XATTRS and collections - but avoid the
        /// complexity of having to actually handle noops requests (and
        /// respond to them) in tests where they are not directly relevent,
        /// as they can arrive in unexpected points if we are not careful in
        /// managing time, or a test is slow, or we are running under a
        /// debugger.
        EnabledButNeverSent,
    };

    /// Configure Noop transmit mode for this DCP Producer.
    void setNoopEnabled(NoopMode mode);

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

    void setMaxMarkerVersion(MarkerVersion value) {
        maxMarkerVersion = value;
    }

    /**
     * Create the ActiveStreamCheckpointProcessorTask and assign to
     * checkpointCreator->task
     */
    void createCheckpointProcessorTask() {
        DcpProducer::createCheckpointProcessorTask();
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
        return log.rlock()->getBytesOutstanding();
    }

    void ackBytesOutstanding(size_t bytes) {
        return log.wlock()->acknowledge(bytes);
    }

    VBReadyQueue& getReadyQueue() {
        return ready;
    }

    /**
     * Place a mock active stream into the producer
     */
    std::shared_ptr<MockActiveStream> mockActiveStreamRequest(
            cb::mcbp::DcpAddStreamFlag flags,
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
            std::function<void(MockActiveStream&)> preSetActiveHook = {});

    std::shared_ptr<MockActiveStream> mockActiveStreamRequest(
            cb::mcbp::DcpAddStreamFlag flags,
            uint32_t opaque,
            VBucket& vb,
            uint64_t start_seqno,
            uint64_t end_seqno,
            uint64_t vbucket_uuid,
            uint64_t snap_start_seqno,
            uint64_t snap_end_seqno,
            IncludeValue includeValue,
            IncludeXattrs includeXattrs,
            IncludeDeletedUserXattrs,
            MarkerVersion maxMarkerVersion,
            std::optional<std::string_view> jsonFilter,
            std::function<void(MockActiveStream&)> preSetActiveHook = {});

    /**
     * Step the producer and expect the opcode to be returned
     */
    cb::engine_errc stepAndExpect(
            MockDcpMessageProducers& producers,
            cb::mcbp::ClientOpcode expectedOpcode,
            cb::engine_errc expectedStatus = cb::engine_errc::success);

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

    uint64_t scheduleBackfillManager(VBucket& vb,
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

    MarkerVersion public_getMaxMarkerVersion() const {
        return maxMarkerVersion;
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

    /**
     * Proxy to DcpProducer::getNextItem()
     */
    std::unique_ptr<DcpResponse> public_getNextItem();

    void public_enableSyncReplication();
};
