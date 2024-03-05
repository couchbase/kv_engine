/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2023-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#pragma once

#include "ep_types.h"

#include <memcached/engine_error.h>
#include <memcached/vbucket.h>

#include <optional>
#include <string_view>
#include <utility>

class MockDcpProducer;
namespace cb::mcbp {
enum class DcpAddStreamFlag : uint32_t;
}

/**
 * Class aims to contain all possible DcpStreamRequest options
 */
class DcpStreamRequestConfig {
public:
    DcpStreamRequestConfig(Vbid vbid,
                           cb::mcbp::DcpAddStreamFlag flags,
                           uint32_t opaque,
                           uint64_t start,
                           uint64_t end,
                           uint64_t snapStart,
                           uint64_t snapEnd,
                           uint64_t uuid,
                           std::optional<std::string_view> collections,
                           cb::engine_errc expectedError);
    /**
     * Stream request on the  given producer with the constructed configuration
     */
    void createDcpStream(MockDcpProducer& producer) const;

private:
    Vbid getVbucket() const;
    uint32_t getOpaque() const;
    cb::mcbp::DcpAddStreamFlag getFlags() const;
    uint64_t getStart() const;
    uint64_t getEnd() const;
    uint64_t getSnapshotStart() const;
    uint64_t getSnapshotEnd() const;
    uint64_t getVbucketUUID() const;
    std::optional<std::string_view> getCollections() const;
    cb::engine_errc getExpectedError() const;

    Vbid vbid;
    cb::mcbp::DcpAddStreamFlag flags;
    uint32_t opaque;
    std::pair<uint64_t, uint64_t> range;
    snapshot_range_t snapshot;
    uint64_t uuid;
    std::optional<std::string_view> collections;
    cb::engine_errc expectedError{cb::engine_errc::success};
};