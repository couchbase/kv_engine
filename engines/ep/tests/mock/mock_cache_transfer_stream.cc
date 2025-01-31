/*
 *     Copyright 2025-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "mock_cache_transfer_stream.h"
#include "dcp/response.h"
#include "mock_dcp_producer.h"

#include <folly/portability/GTest.h>

MockCacheTransferStream::MockCacheTransferStream(
        std::shared_ptr<MockDcpProducer> p,
        uint32_t opaque,
        uint64_t maxSeqno,
        uint64_t vbucketUuid,
        Vbid vbid,
        EventuallyPersistentEngine& engine,
        IncludeValue includeValue)
    : CacheTransferStream(p,
                          "MockCacheTransferStream",
                          opaque,
                          maxSeqno,
                          vbucketUuid,
                          vbid,
                          engine,
                          includeValue) {
}

std::shared_ptr<MockDcpProducer> MockCacheTransferStream::preValidateSteps() {
    auto producer = producerPtr.lock();
    if (!producer) {
        EXPECT_TRUE(producer)
                << "MockCacheTransferStream::preValidateSteps() -> "
                   "Failed producerPtr.lock()";
        return nullptr;
    }

    if (getItemsRemaining() == 0) {
        EXPECT_NE(0, getItemsRemaining())
                << "MockCacheTransferStream::preValidateSteps() -> No "
                   "items";
        return nullptr;
    }

    auto mockProducer = std::dynamic_pointer_cast<MockDcpProducer>(producer);
    if (!mockProducer) {
        EXPECT_TRUE(mockProducer)
                << "MockCacheTransferStream::preValidateSteps() -> Failed "
                   "dynamic_pointer_cast to MockDcpProducer";
        return nullptr;
    }

    return mockProducer;
}

std::unique_ptr<DcpResponse> MockCacheTransferStream::validateNextResponse(
        std::unordered_set<Item>& items) {
    auto producer = preValidateSteps();
    if (!producer) {
        EXPECT_TRUE(producer)
                << "Failed MockCacheTransferStream::validateNextResponse -> "
                   "preValidateSteps()";
        return nullptr;
    }

    auto response = next(*producer);
    EXPECT_TRUE(response);
    if (!response) {
        return nullptr;
    }

    if (response->getEvent() != DcpResponse::Event::Mutation) {
        EXPECT_EQ(DcpResponse::Event::Mutation, response->getEvent())
                << "MockCacheTransferStream::validateNextResponse -> Expected "
                   "a DcpResponse::Event::Mutation, got:"
                << response->to_string();
        return nullptr;
    }
    const auto& mutationResponse =
            *dynamic_cast<MutationResponse*>(response.get());
    auto itr = items.find(*mutationResponse.getItem());
    if (itr == items.end()) {
        EXPECT_FALSE(true)
                << "MockCacheTransferStream::validateNextResponse -> "
                   "Found this unexpected CacheTransferResponse Item in "
                   "the stream. "
                << *mutationResponse.getItem();
    }

    // remove the item from the input set
    items.erase(itr);
    return response;
}

std::unique_ptr<DcpResponse> MockCacheTransferStream::validateNextResponseIsEnd(
        cb::mcbp::DcpStreamEndStatus expectedStatus) {
    auto producer = preValidateSteps();
    if (!producer) {
        EXPECT_TRUE(producer) << "MockCacheTransferStream::"
                                 "validateNextResponseIsEnd -> Failed "
                                 "preValidateSteps()";
        return nullptr;
    }

    auto response = next(*producer);
    EXPECT_TRUE(response);
    if (!response) {
        return nullptr;
    }

    if (response->getEvent() != DcpResponse::Event::StreamEnd) {
        EXPECT_EQ(DcpResponse::Event::StreamEnd, response->getEvent())
                << "MockCacheTransferStream::validateNextResponseIsEnd -> "
                   "Expected "
                   "a "
                   "DcpResponse::Event::StreamEnd, got:"
                << response->to_string();
        return nullptr;
    }
    const auto& streamEndResponse =
            *dynamic_cast<StreamEndResponse*>(response.get());
    if (streamEndResponse.getFlags() != expectedStatus) {
        EXPECT_EQ(expectedStatus, streamEndResponse.getFlags())
                << "MockCacheTransferStream::validateNextResponseIsEnd -> "
                   "Expected "
                   "a "
                   "StreamEndResponse with flags:"
                << cb::mcbp::to_string(expectedStatus) << ", got:"
                << cb::mcbp::to_string(streamEndResponse.getFlags());
        return nullptr;
    }
    return response;
}

size_t MockCacheTransferStream::getMemoryUsed() const {
    return CacheTransferStream::getMemoryUsed() + memoryUsedOffset;
}
