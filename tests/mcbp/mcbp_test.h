/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2015 Couchbase, Inc.
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
