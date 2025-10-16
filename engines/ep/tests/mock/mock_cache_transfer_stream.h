/*
 *     Copyright 2025-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#pragma once

#include "dcp/cache_transfer_stream.h"
#include "dcp/stream_request_info.h"
#include <memcached/vbucket.h>
#include <memory>
#include <unordered_set>

class Item;
class MockDcpProducer;

class MockCacheTransferStream : public CacheTransferStream {
public:
    MockCacheTransferStream(std::shared_ptr<MockDcpProducer> p,
                            uint32_t opaque,
                            const StreamRequestInfo& req,
                            Vbid vbid,
                            EventuallyPersistentEngine& engine,
                            IncludeValue includeValue,
                            Collections::VB::Filter filter);

    /**
     * Perform some checks and then lookup the next response in the stream.
     * It is expected that the stream will return a CacheTransferResponse and
     * that the contained item will be found in the items set. If found in the
     * set, the matching item is removed from the set.
     * @return nullptr if the expectations are not met.
     */
    std::unique_ptr<DcpResponse> validateNextResponse(
            std::unordered_set<Item>& items,
            std::unordered_set<StoredDocKey>* keys = nullptr);

    std::unique_ptr<DcpResponse> validateNextResponseIsEnd(
            cb::mcbp::DcpStreamEndStatus expectedStatus =
                    cb::mcbp::DcpStreamEndStatus::Ok);

    std::unique_ptr<DcpResponse>
    validateNextResponseIsCacheTransferToActiveStream();

    size_t getMemoryUsed() const override;

    // Allows for easy testing of OOM conditions.
    size_t memoryUsedOffset{0};

private:
    std::shared_ptr<MockDcpProducer> preValidateSteps();
};
