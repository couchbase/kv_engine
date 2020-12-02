/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2017 Couchbase, Inc.
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
#include "testapp_client_test.h"
#include <mcbp/mcbp.h>
#include <cctype>
#include <limits>
#include <thread>

/**
 * This test contains tests to verify that we may tune the MCBP SLA
 */
class TuneMcbpSla : public TestappClientTest {
protected:
    std::chrono::nanoseconds getSlowThreshold(cb::mcbp::ClientOpcode opcode);
};

std::chrono::nanoseconds TuneMcbpSla::getSlowThreshold(
        cb::mcbp::ClientOpcode opcode) {
    auto& connection = getAdminConnection();
    auto json = nlohmann::json::parse(connection.ioctl_get("sla"));

    auto iter = json.find(to_string(opcode));
    if (iter == json.end()) {
        // There isn't an explicit entry for the opcode.. there might be
        // default entry we should use instead
        iter = json.find("default");
        if (iter == json.end()) {
            throw std::logic_error(
                    "TuneMcbpSla::getSlowThreshold: No entry for " +
                    to_string(opcode));
        }
    }

    return cb::mcbp::sla::getSlowOpThreshold(*iter);
}

INSTANTIATE_TEST_SUITE_P(TransportProtocols,
                         TuneMcbpSla,
                         ::testing::Values(TransportProtocols::McbpSsl),
                         ::testing::PrintToStringParamName());

TEST_P(TuneMcbpSla, NoAccess) {
    auto& connection = getConnection();
    try {
        connection.ioctl_set("sla", R"({"version":1})");
        FAIL() << "Normal users should not be able to set the SLA";
    } catch (const ConnectionError& e) {
        EXPECT_TRUE(e.isAccessDenied());
    }
}

TEST_P(TuneMcbpSla, InvalidPayload) {
    auto& connection = getAdminConnection();

    // No payload isn't allowed
    try {
        connection.ioctl_set("sla", "");
        FAIL() << "An empty input string is not allowed";
    } catch (const ConnectionError& e) {
        EXPECT_TRUE(e.isInvalidArguments());
    }

    // It must be JSON
    try {
        connection.ioctl_set("sla", "asdfasdff");
        FAIL() << "The data must be JSON";
    } catch (const ConnectionError& e) {
        EXPECT_TRUE(e.isInvalidArguments());
    }

    // It must contain the version tag
    try {
        connection.ioctl_set("sla", "{}");
        FAIL() << "The data must contain the version tag";
    } catch (const ConnectionError& e) {
        EXPECT_TRUE(e.isInvalidArguments());
    }
}

TEST_P(TuneMcbpSla, Update) {
    // Try to set everything to 500ms (note that this don't really check
    // if it works if that's the same as the server's default.. but we're
    // trying again later on..
    getAdminConnection().ioctl_set("sla",
                                   R"({"version":1, "default":{"slow":500}})");
    EXPECT_EQ(std::chrono::milliseconds(500),
              getSlowThreshold(cb::mcbp::ClientOpcode::Get));
    EXPECT_EQ(std::chrono::milliseconds(500),
              getSlowThreshold(cb::mcbp::ClientOpcode::Set));

    getAdminConnection().ioctl_set("sla",
                                   R"({"version":1, "set":{"slow":100}})");

    EXPECT_EQ(std::chrono::milliseconds(500),
              getSlowThreshold(cb::mcbp::ClientOpcode::Get));
    EXPECT_EQ(std::chrono::milliseconds(100),
              getSlowThreshold(cb::mcbp::ClientOpcode::Set));

    // Verify that setting default sets all of them
    getAdminConnection().ioctl_set("sla",
                                   R"({"version":1, "default":{"slow":500}})");
    EXPECT_EQ(std::chrono::milliseconds(500),
              getSlowThreshold(cb::mcbp::ClientOpcode::Get));
    EXPECT_EQ(std::chrono::milliseconds(500),
              getSlowThreshold(cb::mcbp::ClientOpcode::Set));
}
