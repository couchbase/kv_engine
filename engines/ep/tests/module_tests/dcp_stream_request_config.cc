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

#include "tests/module_tests/dcp_stream_request_config.h"
#include "tests/mock/mock_dcp_producer.h"

#include <folly/portability/GTest.h>

DcpStreamRequestConfig::DcpStreamRequestConfig(
        Vbid vbid,
        cb::mcbp::DcpAddStreamFlag flags,
        uint32_t opaque,
        uint64_t start,
        uint64_t end,
        uint64_t snapStart,
        uint64_t snapEnd,
        uint64_t uuid,
        std::optional<std::string_view> collections,
        cb::engine_errc expectedError)
    : vbid(vbid),
      flags(flags),
      opaque(opaque),
      range{std::make_pair(start, end)},
      snapshot(snapStart, snapEnd),
      uuid(uuid),
      collections(collections),
      expectedError{expectedError} {
}

Vbid DcpStreamRequestConfig::getVbucket() const {
    return vbid;
}

cb::mcbp::DcpAddStreamFlag DcpStreamRequestConfig::getFlags() const {
    return flags;
}

uint32_t DcpStreamRequestConfig::getOpaque() const {
    return opaque;
}

uint64_t DcpStreamRequestConfig::getStart() const {
    return range.first;
}

uint64_t DcpStreamRequestConfig::getEnd() const {
    return range.second;
}

uint64_t DcpStreamRequestConfig::getSnapshotStart() const {
    return snapshot.getStart();
}

uint64_t DcpStreamRequestConfig::getSnapshotEnd() const {
    return snapshot.getEnd();
}

uint64_t DcpStreamRequestConfig::getVbucketUUID() const {
    return uuid;
}

std::optional<std::string_view> DcpStreamRequestConfig::getCollections() const {
    return collections;
}

cb::engine_errc DcpStreamRequestConfig::getExpectedError() const {
    return expectedError;
}

void DcpStreamRequestConfig::createDcpStream(MockDcpProducer& producer) const {
    uint64_t rollbackSeqno{0};
    EXPECT_EQ(getExpectedError(),
              producer.streamRequest(
                      getFlags(),
                      getOpaque(),
                      getVbucket(),
                      getStart(),
                      getEnd(),
                      getVbucketUUID(),
                      getSnapshotStart(),
                      getSnapshotEnd(),
                      0,
                      &rollbackSeqno,
                      [](const std::vector<vbucket_failover_t>&) {
                          return cb::engine_errc::success;
                      },
                      getCollections()));
}
