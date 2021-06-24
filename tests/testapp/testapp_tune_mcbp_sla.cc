/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2017-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#include "testapp_client_test.h"
#include <boost/filesystem.hpp>
#include <mcbp/mcbp.h>
#include <platform/dirutils.h>
#include <utilities/string_utilities.h>
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

TEST_P(TuneMcbpSla, SlowCommandLogging) {
    TESTAPP_SKIP_FOR_OTHER_BUCKETS(BucketType::Couchbase);
    auto& conn = getAdminConnection();
    conn.ioctl_set(
            "sla",
            R"({"version":1, "compact_db":{"slow":1}, "default":{"slow":500}})");
    conn.selectBucket("default");
    const auto rsp = conn.execute(BinprotCompactDbCommand());
    EXPECT_TRUE(rsp.isSuccess()) << rsp.getDataString();

    // "grep" like function to pick out the lines in all the log files
    // containing ": Slow operation: "
    auto findLogLines = []() {
        std::vector<std::string> ret;
        for (const auto& p :
             boost::filesystem::directory_iterator(mcd_env->getLogDir())) {
            if (is_regular_file(p)) {
                auto lines = split_string(
                        cb::io::loadFile(p.path().generic_string()), "\n");
                for (auto& l : lines) {
                    if (l.find(": Slow operation: ") != std::string::npos) {
                        ret.emplace_back(std::move(l));
                    }
                }
            }
        }
        return ret;
    };

    // Logging is asynchronous so there are no guarantee that the entry exists
    // in the logfiles already. We can't wait forever as that would cause
    // a failing test to hang for a long time. It _should_ be relatively quick
    // so lets set a timeout for 30 sec (so we don't get false positives on
    // an overloaded CV slave running too many jobs in parallel)
    const auto timeout =
            std::chrono::steady_clock::now() + std::chrono::seconds{30};

    do {
        const auto entries = findLogLines();
        if (!entries.empty()) {
            // Verify that it has the correct format
            EXPECT_EQ(1, entries.size());
            for (const auto& entry : entries) {
                auto idx = entry.find('{');
                ASSERT_NE(std::string::npos, idx);
                auto json = nlohmann::json::parse(entry.substr(idx));
                EXPECT_EQ("COMPACT_DB", json["command"].get<std::string>());
                EXPECT_EQ("Success", json["response"].get<std::string>());
            }
            return;
        }

        std::this_thread::sleep_for(std::chrono::milliseconds{20});
    } while (std::chrono::steady_clock::now() < timeout);
    FAIL() << "Timed out before the slow log appeared in the files";
}
