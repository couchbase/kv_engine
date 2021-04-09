/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2018-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "mock_dcp_producer.h"

#include "dcp/active_stream_checkpoint_processor_task.h"
#include "dcp/msg_producers_border_guard.h"
#include "dcp/response.h"
#include "mock_bucket_logger.h"
#include "mock_dcp.h"
#include "mock_dcp_backfill_mgr.h"
#include "mock_stream.h"
#include "vbucket.h"

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
        IncludeXattrs includeXattrs,
        IncludeDeletedUserXattrs includeDeletedUserXattrs,
        std::optional<std::string_view> jsonFilter) {
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
            includeXattrs,
            includeDeletedUserXattrs,
            jsonFilter);
    stream->setActive();

    auto baseStream = std::dynamic_pointer_cast<ActiveStream>(stream);
    updateStreamsMap(vb.getId(), stream->getStreamId(), baseStream);

    auto found = streams->find(vb.getId().get());
    if (found == streams->end()) {
        throw std::logic_error(
                "MockDcpProducer::mockActiveStreamRequest "
                "failed to insert requested stream");
    }
    notifyStreamReady(vb.getId());
    return stream;
}

cb::engine_errc MockDcpProducer::stepAndExpect(
        MockDcpMessageProducers& producers,
        cb::mcbp::ClientOpcode expectedOpcode) {
    auto rv = step(producers);
    EXPECT_EQ(expectedOpcode, producers.last_op);
    return rv;
}

cb::engine_errc MockDcpProducer::stepWithBorderGuard(
        DcpMessageProducersIface& producers) {
    DcpMsgProducersBorderGuard guardedProducers(producers);
    return step(guardedProducers);
}

std::shared_ptr<ActiveStream> MockDcpProducer::findStream(Vbid vbid) {
    auto rv = streams->find(vbid.get());
    if (rv != streams->end()) {
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

std::pair<std::shared_ptr<ActiveStream>, bool> MockDcpProducer::findStream(
        Vbid vbid, cb::mcbp::DcpStreamId sid) {
    auto rv = streams->find(vbid.get());
    if (rv != streams->end()) {
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
    return std::dynamic_pointer_cast<MockDcpBackfillManager>(backfillMgr)
            ->setBackfillBufferSize(newSize);
}

void MockDcpProducer::setBackfillBufferBytesRead(size_t newSize) {
    return std::dynamic_pointer_cast<MockDcpBackfillManager>(backfillMgr)
            ->setBackfillBufferBytesRead(newSize);
}

bool MockDcpProducer::getBackfillBufferFullStatus() {
    return std::dynamic_pointer_cast<MockDcpBackfillManager>(backfillMgr)
            ->getBackfillBufferFullStatus();
}

BackfillScanBuffer& MockDcpProducer::public_getBackfillScanBuffer() {
    return std::dynamic_pointer_cast<MockDcpBackfillManager>(backfillMgr)
            ->public_getBackfillScanBuffer();
}

void MockDcpProducer::setupMockLogger() {
    logger = std::make_shared<::testing::NiceMock<MockBucketLogger>>("prod");
}

MockBucketLogger& MockDcpProducer::public_getLogger() const {
    EXPECT_TRUE(logger);
    return dynamic_cast<MockBucketLogger&>(*logger);
}
