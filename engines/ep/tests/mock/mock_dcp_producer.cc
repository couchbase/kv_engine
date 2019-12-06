/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2018 Couchbase, Inc.
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

#include "mock_dcp_producer.h"

#include "dcp/active_stream_checkpoint_processor_task.h"
#include "dcp/msg_producers_border_guard.h"
#include "dcp/response.h"
#include "mock_dcp.h"
#include "mock_dcp_backfill_mgr.h"
#include "mock_stream.h"
#include "vbucket.h"

#include <folly/portability/GTest.h>

extern cb::mcbp::ClientOpcode last_op;

MockDcpProducer::MockDcpProducer(EventuallyPersistentEngine& theEngine,
                                 const void* cookie,
                                 const std::string& name,
                                 uint32_t flags,
                                 bool startTask)
    : DcpProducer(theEngine, cookie, name, flags, startTask) {
    backfillMgr = std::make_shared<MockDcpBackfillManager>(engine_);
}

std::shared_ptr<MockActiveStream> MockDcpProducer::mockActiveStreamRequest(
        uint32_t flags,
        uint32_t opaque,
        VBucket& vb,
        uint64_t start_seqno,
        uint64_t end_seqno,
        uint64_t vbucket_uuid,
        uint64_t snap_start_seqno,
        uint64_t snap_end_seqno,
        IncludeValue includeValue,
        IncludeXattrs includeXattrs) {
    auto stream = std::make_shared<MockActiveStream>(
            static_cast<EventuallyPersistentEngine*>(&engine_),
            std::static_pointer_cast<MockDcpProducer>(shared_from_this()),
            flags,
            opaque,
            vb,
            start_seqno,
            end_seqno,
            vbucket_uuid,
            snap_start_seqno,
            snap_end_seqno,
            includeValue,
            includeXattrs);
    stream->setActive();

    auto baseStream = dynamic_pointer_cast<Stream>(stream);
    updateStreamsMap(vb.getId(), stream->getStreamId(), baseStream);

    auto found = streams.find(vb.getId().get());
    if (found == streams.end()) {
        throw std::logic_error(
                "MockDcpProducer::mockActiveStreamRequest "
                "failed to insert requested stream");
    }
    notifyStreamReady(vb.getId());
    return stream;
}

ENGINE_ERROR_CODE MockDcpProducer::stepAndExpect(
        MockDcpMessageProducers* producers,
        cb::mcbp::ClientOpcode expectedOpcode) {
    auto rv = step(producers);
    EXPECT_EQ(expectedOpcode, producers->last_op);
    return rv;
}

ENGINE_ERROR_CODE MockDcpProducer::stepWithBorderGuard(
        dcp_message_producers& producers) {
    DcpMsgProducersBorderGuard guardedProducers(producers);
    return step(&guardedProducers);
}

std::shared_ptr<Stream> MockDcpProducer::findStream(Vbid vbid) {
    auto rv = streams.find(vbid.get());
    if (rv != streams.end()) {
        auto handle = rv->second->rlock();
        // An empty StreamContainer for this vbid is allowed
        if (handle.size() == 0) {
            return nullptr;
        }

        if (handle.size() != 1) {
            throw std::logic_error(
                    "MockDcpProducer::findStream against producer with many "
                    "streams size:" +
                    std::to_string(handle.size()));
        }
        return handle.get();
    }
    return nullptr;
}

ActiveStreamCheckpointProcessorTask*
MockDcpProducer::getCheckpointSnapshotTask() const {
    LockHolder guard(checkpointCreator->mutex);
    return static_cast<ActiveStreamCheckpointProcessorTask*>(
            checkpointCreator->task.get());
}

std::pair<std::shared_ptr<Stream>, bool> MockDcpProducer::findStream(
        Vbid vbid, cb::mcbp::DcpStreamId sid) {
    auto rv = streams.find(vbid.get());
    if (rv != streams.end()) {
        auto handle = rv->second->rlock();
        // Try and locate a matching stream
        for (; !handle.end(); handle.next()) {
            if (handle.get()->compareStreamId(sid)) {
                return {handle.get(), true};
            }
        }
        return {nullptr, handle.size() > 0};
    }
    return {nullptr, false};
}

void MockDcpProducer::setBackfillBufferSize(size_t newSize) {
    return std::dynamic_pointer_cast<MockDcpBackfillManager>(backfillMgr.load())
            ->setBackfillBufferSize(newSize);
}

bool MockDcpProducer::getBackfillBufferFullStatus() {
    return std::dynamic_pointer_cast<MockDcpBackfillManager>(backfillMgr.load())
            ->getBackfillBufferFullStatus();
}

BackfillScanBuffer& MockDcpProducer::public_getBackfillScanBuffer() {
    return std::dynamic_pointer_cast<MockDcpBackfillManager>(backfillMgr.load())
            ->public_getBackfillScanBuffer();
}

void MockDcpProducer::bytesForceRead(size_t bytes) {
    backfillMgr->bytesForceRead(bytes);
}
