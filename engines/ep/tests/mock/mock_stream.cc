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

#include "mock_stream.h"
#include "checkpoint_manager.h"
#include "dcp/consumer.h"
#include "dcp/response.h"
#include "mock_dcp_producer.h"
#include "vbucket.h"
MockActiveStream::MockActiveStream(
        EventuallyPersistentEngine* e,
        std::shared_ptr<MockDcpProducer> p,
        cb::mcbp::DcpAddStreamFlag flags,
        uint32_t opaque,
        VBucket& vb,
        uint64_t st_seqno,
        uint64_t en_seqno,
        uint64_t vb_uuid,
        uint64_t snap_start_seqno,
        uint64_t snap_end_seqno,
        IncludeValue includeValue,
        IncludeXattrs includeXattrs,
        IncludeDeletedUserXattrs includeDeletedUserXattrs,
        std::optional<std::string_view> jsonFilter,
        const std::string& streamName)
    : ActiveStream(e,
                   p,
                   streamName.empty() ? p->getName() : streamName,
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
                   IncludeDeleteTime::No,
                   includeDeletedUserXattrs,
                   MarkerVersion::V2_0,
                   {jsonFilter, vb.getManifest(), *p->getCookie(), *e}) {
}

MockActiveStream::MockActiveStream(
        EventuallyPersistentEngine* e,
        std::shared_ptr<MockDcpProducer> p,
        cb::mcbp::DcpAddStreamFlag flags,
        uint32_t opaque,
        VBucket& vb,
        uint64_t st_seqno,
        uint64_t en_seqno,
        uint64_t vb_uuid,
        uint64_t snap_start_seqno,
        uint64_t snap_end_seqno,
        IncludeValue includeValue,
        IncludeXattrs includeXattrs,
        IncludeDeletedUserXattrs includeDeletedUserXattrs,
        MarkerVersion maxMarkerVersion,
        std::optional<std::string_view> jsonFilter,
        const std::string& streamName)
    : ActiveStream(e,
                   p,
                   streamName.empty() ? p->getName() : streamName,
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
                   IncludeDeleteTime::No,
                   includeDeletedUserXattrs,
                   maxMarkerVersion,
                   {jsonFilter, vb.getManifest(), *p->getCookie(), *e}) {
}

void MockActiveStream::public_registerCursor(CheckpointManager& manager,
                                             const std::string& name,
                                             int64_t seqno) {
    auto registerResult = manager.registerCursorBySeqno(
            name, seqno, CheckpointCursor::Droppable::Yes);
    cursor = registerResult.takeCursor();
}

std::unique_ptr<DcpResponse> MockActiveStream::public_popFromReadyQ() {
    std::lock_guard<std::mutex> lg(streamMutex);
    return popFromReadyQ();
}

std::unique_ptr<DcpResponse> MockActiveStream::public_nextQueuedItem(
        DcpProducer& producer) {
    std::lock_guard<std::mutex> lh(streamMutex);
    return nextQueuedItem(producer);
}

std::unique_ptr<DcpResponse> MockActiveStream::public_makeResponseFromItem(
        queued_item& item, SendCommitSyncWriteAs sendMutationInsteadOfCommit) {
    return makeResponseFromItem(item, sendMutationInsteadOfCommit);
}

void MockActiveStream::consumeBackfillItems(DcpProducer& producer,
                                            int numItems) {
    std::lock_guard<std::mutex> lh(streamMutex);
    for (int items = 0; items < numItems;) {
        auto resp = backfillPhase(producer, lh);
        if (resp) {
            ++items;
        }
    }
}
void MockActiveStream::consumeAllBackfillItems(DcpProducer& producer) {
    std::lock_guard<std::mutex> lh(streamMutex);
    while (backfillPhase(producer, lh)) {
    }
}

std::unique_ptr<DcpResponse> MockActiveStream::public_backfillPhase(
        DcpProducer& producer) {
    if (state_.load() != StreamState::Backfilling) {
        throw std::runtime_error(
                "MockPassiveStream::public_backfillPhase: not backfilling!");
    }
    std::lock_guard<std::mutex> lh(streamMutex);
    return backfillPhase(producer, lh);
}

std::unique_ptr<DcpResponse> MockActiveStream::public_inMemoryPhase(
        DcpProducer& producer) {
    if (state_.load() != StreamState::InMemory) {
        throw std::runtime_error(
                "MockPassiveStream::public_inMemoryPhase: not backfilling!");
    }
    return inMemoryPhase(producer);
}

cb::engine_errc MockPassiveStream::messageReceived(
        std::unique_ptr<DcpResponse> dcpResponse) {
    auto consumer = consumerPtr.lock();
    if (!consumer) {
        throw std::logic_error(
                "MockPassiveStream::messageReceived cannot proceed without the "
                "DcpConsumer");
    }
    UpdateFlowControl ufc(*consumer, dcpResponse->getMessageSize());
    return PassiveStream::messageReceived(std::move(dcpResponse), ufc);
}

std::unique_ptr<DcpResponse> MockPassiveStream::public_popFromReadyQ() {
    std::lock_guard<std::mutex> lg(streamMutex);
    return popFromReadyQ();
}
