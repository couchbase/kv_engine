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
        const StreamRequestInfo& req,
        Vbid vbid,
        EventuallyPersistentEngine& engine,
        Collections::VB::Filter filter)
    : CacheTransferStream(p,
                          "MockCacheTransferStream",
                          opaque,
                          req,
                          vbid,
                          engine,
                          std::move(filter)) {
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

std::unique_ptr<DcpResponse> MockCacheTransferStream::validateNextResponse() {
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

    if (response->getEvent() != DcpResponse::Event::CacheTransfer) {
        EXPECT_EQ(DcpResponse::Event::CacheTransfer, response->getEvent())
                << "MockCacheTransferStream::validateNextResponse -> Expected "
                   "a DcpResponse::Event::CacheTransfer event, got:"
                << response->to_string();
        return nullptr;
    }
    return response;
}

std::unique_ptr<DcpResponse> MockCacheTransferStream::validateNextResponse(
        std::unordered_set<Item>& items, std::unordered_set<Item>* keysMeta) {
    auto response = validateNextResponse();
    if (!response) {
        return nullptr;
    }

    auto* ctResponse = dynamic_cast<DcpCacheTransfer*>(response.get());
    EXPECT_NE(nullptr, ctResponse)
            << "MockCacheTransferStream::validateNextResponse -> Failed "
            << "dynamic_cast to CacheTransferResponse";
    if (!ctResponse) {
        return nullptr;
    }

    // Iterate each item from the batch:
    // 1) An Item with a value is expected to be found in the provided `items`,
    // and then removed from that set.
    // 2) An Item without a value is expected to be found in `keysMeta` and then
    // removed from that set.
    for (auto& iwh : ctResponse->getItems()) {
        auto& item = *reinterpret_cast<Item*>(iwh.item.get());
        if (item.getNBytes() > 0) {
            // Case1: Item with value, should be found in `items`
            auto itr = items.find(item);
            if (itr == items.end()) {
                ADD_FAILURE() << "MockCacheTransferStream::"
                                 "validateNextResponse -> stream produced an "
                                 "item which is not found in the expected set:"
                              << item;
            } else {
                items.erase(itr);
            }
        } else {
            // Case 2: Item without value, should be found in `keysMeta` (and
            // keysMeta must be provided in this case)
            EXPECT_NE(nullptr, keysMeta)
                    << "MockCacheTransferStream::validateNextResponse -> "
                       "received a key/meta-only item but keysMeta set was "
                       "not provided";
            if (!keysMeta) {
                continue;
            }
            auto itr = keysMeta->find(item);
            if (itr == keysMeta->end()) {
                ADD_FAILURE() << "MockCacheTransferStream::"
                                 "validateNextResponse -> stream produced an "
                                 "item (key only case) which is not found in "
                                 "the expected set:"
                              << item;
            } else {
                keysMeta->erase(itr);
            }
        }
    }
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
                   "Expected a DcpResponse::Event::StreamEnd, got:"
                << response->to_string();
        return nullptr;
    }
    const auto& streamEndResponse =
            *dynamic_cast<StreamEndResponse*>(response.get());
    if (streamEndResponse.getFlags() != expectedStatus) {
        EXPECT_EQ(expectedStatus, streamEndResponse.getFlags())
                << "MockCacheTransferStream::validateNextResponseIsEnd -> "
                   "Expected a StreamEndResponse with flags:"
                << cb::mcbp::to_string(expectedStatus) << ", got:"
                << cb::mcbp::to_string(streamEndResponse.getFlags());
        return nullptr;
    }
    return response;
}

size_t MockCacheTransferStream::getMemoryUsed() const {
    return CacheTransferStream::getMemoryUsed() + memoryUsedOffset;
}

std::unique_ptr<DcpResponse>
MockCacheTransferStream::validateNextResponseIsCacheTransferToActiveStream() {
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

    if (response->getEvent() !=
        DcpResponse::Event::CacheTransferToActiveStream) {
        EXPECT_EQ(DcpResponse::Event::CacheTransferToActiveStream,
                  response->getEvent())
                << "MockCacheTransferStream::validateNextResponse -> Expected "
                   "a DcpResponse::Event::CacheTransferToActiveStream, got:"
                << response->to_string();
        return nullptr;
    }

    return response;
}
