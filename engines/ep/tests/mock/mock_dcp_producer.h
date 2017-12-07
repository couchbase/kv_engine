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

#include "collections/manager.h"
#include "dcp/producer.h"
#include "dcp/stream.h"
#include "mock_dcp_backfill_mgr.h"

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
                    cb::const_byte_buffer jsonExtra,
                    bool startTask = true)
        : DcpProducer(
                  theEngine,
                  cookie,
                  name,
                  flags,
                  theEngine.getKVBucket()->getCollectionsManager().makeFilter(
                          flags, jsonExtra),
                  startTask) {
        backfillMgr.reset(new MockDcpBackfillManager(engine_));
    }

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

    /**
     * Create the ActiveStreamCheckpointProcessorTask and assign to
     * checkpointCreatorTask
     */
    void createCheckpointProcessorTask() {
        DcpProducer::createCheckpointProcessorTask();
    }

    /**
     * Schedule the checkpointCreatorTask on the ExecutorPool
     */
    void scheduleCheckpointProcessorTask() {
        DcpProducer::scheduleCheckpointProcessorTask();
    }

    ActiveStreamCheckpointProcessorTask& getCheckpointSnapshotTask() const {
        return *static_cast<ActiveStreamCheckpointProcessorTask*>(
                checkpointCreatorTask.get());
    }

    /**
     * Finds the stream for a given vbucket
     */
    std::shared_ptr<Stream> findStream(uint16_t vbid) {
        return DcpProducer::findStream(vbid);
    }

    std::string getCurrentSeparatorForStream(uint16_t vbid) {
        auto stream = findStream(vbid);
        if (stream) {
            auto as = static_cast<ActiveStream*>(stream.get());
            return as->getCurrentSeparator();
        }
        return {};
    }

    /**
     * Sets the backfill buffer size (max limit) to a particular value
     */
    void setBackfillBufferSize(size_t newSize) {
        dynamic_cast<MockDcpBackfillManager*>(backfillMgr.get())
                ->setBackfillBufferSize(newSize);
    }

    bool getBackfillBufferFullStatus() {
        return dynamic_cast<MockDcpBackfillManager*>(backfillMgr.get())
                ->getBackfillBufferFullStatus();
    }

    const Collections::Filter& getFilter() {
        return filter;
    }

    void bytesForceRead(size_t bytes) {
        backfillMgr->bytesForceRead(bytes);
    }

    BackfillManager& getBFM() {
        return *backfillMgr;
    }
};
