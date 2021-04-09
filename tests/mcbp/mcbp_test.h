/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2015-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#pragma once

/*
 * Memcached binary protocol validator tests.
 */
#include "mock_connection.h"

#include <daemon/connection.h>
#include <daemon/mcbp_validators.h>
#include <daemon/stats.h>
#include <folly/portability/GTest.h>
#include <memcached/protocol_binary.h>

namespace mcbp {
namespace test {

class ValidatorTest : public ::testing::Test {
public:
    explicit ValidatorTest(bool collectionsEnabled);
    void SetUp() override;

protected:
    /**
     * Validate that the provided packet is correctly encoded
     *
     * @param opcode The opcode for the packet
     * @param request The packet to validate
     */
    cb::mcbp::Status validate(cb::mcbp::ClientOpcode opcode, void* request);
    std::string validate_error_context(
            cb::mcbp::ClientOpcode opcode,
            void* request,
            cb::mcbp::Status expectedStatus = cb::mcbp::Status::Einval);
    std::string validate_error_context(
            cb::mcbp::ClientOpcode opcode,
            cb::mcbp::Status expectedStatus = cb::mcbp::Status::Einval);

    McbpValidator validatorChains;

    MockConnection connection;

    // backing store which may be used for the request
    protocol_binary_request_no_extras &request;
    uint8_t blob[4096] = {0};

    bool collectionsEnabled{false};
};

} // namespace test
} // namespace mcbp
