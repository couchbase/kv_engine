/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2016 Couchbase, Inc.
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

#include "dcp/active_stream.h"
#include "dcp/producer.h"
#include "dcp/stream.h"

class ActiveStreamCheckpointProcessorTask;
struct BackfillScanBuffer;
class MockActiveStream;
class MockDcpMessageProducers;

/*
 * Mock of the DcpProducer class.  Wraps the real DcpProducer, but exposes
 * normally protected methods publically for test purposes.
 */
class MockDcpProducer : public DcpProducer {
public:
    MockDcpProducer(EventuallyPersistentEngine& theEngine,
                    const void* cookie,
                    const std::string& name,
                    uint32_t flags,
                    bool startTask = true);

    using DcpProducer::updateStreamsMap;

    ENGINE_ERROR_CODE maybeDisconnect() {
        return DcpProducer::maybeDisconnect();
    }

    ENGINE_ERROR_CODE maybeSendNoop(struct dcp_message_producers* producers) {
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
    std::shared_ptr<Stream> findStream(Vbid vbid);

    /**
     * Finds the stream for a given vbucket/sid
     * @returns a pair where second indicates if the VB has no entries at all
     *          first is the stream (or null if no stream)
     */
    std::pair<std::shared_ptr<Stream>, bool> findStream(
            Vbid vbid, cb::mcbp::DcpStreamId sid);

    /**
     * Sets the backfill buffer size (max limit) to a particular value
     */
    void setBackfillBufferSize(size_t newSize);

    bool getBackfillBufferFullStatus();

    /*
     * @return A reference to BackfillManager::scanBuffer
     */
    BackfillScanBuffer& public_getBackfillScanBuffer();

    void bytesForceRead(size_t bytes);

    BackfillManager& getBFM() {
        return *(backfillMgr.load());
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
            IncludeXattrs includeXattrs = IncludeXattrs::Yes);

    /**
     * Step the producer and expect the opcode to be returned
     */
    ENGINE_ERROR_CODE stepAndExpect(MockDcpMessageProducers* producers,
                                    cb::mcbp::ClientOpcode expectedOpcode);

    /**
     * Call step(), but wrap the producers with a DcpMsgProducersBorderGuard (
     * in the same way it is called from EvPEngine::step().
     *
     * Useful when operating directly on a (Mock)DcpProducer object without
     * ep_engine, but need to ensure currentEngine switching is still correct.
     */
    ENGINE_ERROR_CODE stepWithBorderGuard(dcp_message_producers& producers);

    void enableMultipleStreamRequests() {
        multipleStreamRequests = MultipleStreamRequests::Yes;
    }

    void enableStreamEndOnClientStreamClose() {
        sendStreamEndOnClientStreamClose = true;
    }
};
