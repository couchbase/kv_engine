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
#include <folly/portability/GTest.h>
#include <mcbp/mcbp.h>
#include <nlohmann/json.hpp>

TEST(McbpSlaReconfig, MissingVersion) {
    try {
        cb::mcbp::sla::reconfigure(nlohmann::json::object());
        FAIL() << "version is a mandatory element";
    } catch (const std::invalid_argument& e) {
        EXPECT_STREQ(
                "cb::mcbp::sla::reconfigure: Missing mandatory element "
                "'version'",
                e.what());
    }
}

TEST(McbpSlaReconfig, IncorrectVersionType) {
    const auto doc = R"({"version": true})"_json;
    try {
        cb::mcbp::sla::reconfigure(doc);
        FAIL() << "version should be a number";
    } catch (const std::invalid_argument& e) {
        EXPECT_STREQ("cb::mcbp::sla::reconfigure: 'version' should be a number",
                     e.what());
    }
}

TEST(McbpSlaReconfig, UnsupportedVersion) {
    const auto doc = R"({"version": 2})"_json;
    try {
        cb::mcbp::sla::reconfigure(doc);
        FAIL() << "version should be 1";
    } catch (const std::invalid_argument& e) {
        EXPECT_STREQ("cb::mcbp::sla::reconfigure: Unsupported version: 2",
                     e.what());
    }
}

TEST(McbpSlaReconfig, SupportedVersion) {
    const auto doc = R"({"version": 1})"_json;
    cb::mcbp::sla::reconfigure(doc);
}

TEST(McbpSlaReconfig, DefaultEntryNotAnObject) {
    const auto doc = R"({"version": 1, "default": false})"_json;
    try {
        cb::mcbp::sla::reconfigure(doc);
        FAIL() << "Default must be an object";
    } catch (const std::invalid_argument& e) {
        EXPECT_STREQ(
                "cb::mcbp::sla::getSlowOpThreshold: Entry is not an object",
                e.what());
    }
}

TEST(McbpSlaReconfig, DefaultObjectMissingSlow) {
    nlohmann::json doc = R"({"version": 1, "default": {}})"_json;
    try {
        cb::mcbp::sla::reconfigure(doc);
        FAIL() << "Default must contain 'slow'";
    } catch (const std::invalid_argument& e) {
        EXPECT_STREQ(
                "cb::mcbp::sla::getSlowOpThreshold: Entry does not "
                "contain a mandatory 'slow' entry",
                e.what());
    }
}

TEST(McbpSlaReconfig, LegalDefaultObject) {
    const auto doc = R"({"version": 1, "default": {"slow": 500}})"_json;
    cb::mcbp::sla::reconfigure(doc);
}

TEST(McbpSlaReconfig, UnknownCommand) {
    const auto doc = R"({"version": 1, "foo": {"slow": 500}})"_json;

    try {
        cb::mcbp::sla::reconfigure(doc);
        FAIL() << "Should not allow unknown commands";
    } catch (const std::invalid_argument& e) {
        EXPECT_STREQ("cb::mcbp::sla::reconfigure: Unknown command 'foo'",
                     e.what());
    }
}

TEST(McbpSlaReconfig, GetEntryNotAnObject) {
    const auto doc = R"({"version": 1, "get": false})"_json;
    try {
        cb::mcbp::sla::reconfigure(doc);
        FAIL() << "Entries must be an object";
    } catch (const std::invalid_argument& e) {
        EXPECT_STREQ(
                "cb::mcbp::sla::getSlowOpThreshold: Entry is not an object",
                e.what());
    }
}

TEST(McbpSlaReconfig, GetObjectMissingSlow) {
    const auto doc = R"({"version": 1, "get": {}})"_json;
    try {
        cb::mcbp::sla::reconfigure(doc);
        FAIL() << "Entries must contain 'slow'";
    } catch (const std::invalid_argument& e) {
        EXPECT_STREQ(
                "cb::mcbp::sla::getSlowOpThreshold: Entry does not "
                "contain a mandatory 'slow' entry",
                e.what());
    }
}

TEST(McbpSlaReconfig, LegalGetObject) {
    const auto doc = R"({"version": 1, "get": {"slow": 500}})"_json;
    cb::mcbp::sla::reconfigure(doc);
}

// Test all of the allowed permutations of string values to specify a time
// gap
TEST(McbpSlaReconfig, GetParseStringValues) {
    auto doc = R"({"version": 1, "get": {"slow": "1 ns"}})"_json;
    cb::mcbp::sla::reconfigure(doc);
    EXPECT_EQ(std::chrono::nanoseconds(1),
              cb::mcbp::sla::getSlowOpThreshold(cb::mcbp::ClientOpcode::Get));

    doc = R"({"version": 1, "get": {"slow": "2 nanoseconds"}})"_json;
    cb::mcbp::sla::reconfigure(doc);
    EXPECT_EQ(std::chrono::nanoseconds(2),
              cb::mcbp::sla::getSlowOpThreshold(cb::mcbp::ClientOpcode::Get));

    doc = R"({"version": 1, "get": {"slow": "1 us"}})"_json;
    cb::mcbp::sla::reconfigure(doc);
    EXPECT_EQ(std::chrono::microseconds(1),
              cb::mcbp::sla::getSlowOpThreshold(cb::mcbp::ClientOpcode::Get));

    doc = R"({"version": 1, "get": {"slow": "2 microseconds"}})"_json;
    cb::mcbp::sla::reconfigure(doc);
    EXPECT_EQ(std::chrono::microseconds(2),
              cb::mcbp::sla::getSlowOpThreshold(cb::mcbp::ClientOpcode::Get));

    doc = R"({"version": 1, "get": {"slow": "1 ms"}})"_json;
    cb::mcbp::sla::reconfigure(doc);
    EXPECT_EQ(std::chrono::milliseconds(1),
              cb::mcbp::sla::getSlowOpThreshold(cb::mcbp::ClientOpcode::Get));

    doc = R"({"version": 1, "get": {"slow": "2 milliseconds"}})"_json;
    cb::mcbp::sla::reconfigure(doc);
    EXPECT_EQ(std::chrono::milliseconds(2),
              cb::mcbp::sla::getSlowOpThreshold(cb::mcbp::ClientOpcode::Get));

    doc = R"({"version": 1, "get": {"slow": "1 s"}})"_json;
    cb::mcbp::sla::reconfigure(doc);
    EXPECT_EQ(std::chrono::seconds(1),
              cb::mcbp::sla::getSlowOpThreshold(cb::mcbp::ClientOpcode::Get));

    doc = R"({"version": 1, "get": {"slow": "2 seconds"}})"_json;
    cb::mcbp::sla::reconfigure(doc);
    EXPECT_EQ(std::chrono::seconds(2),
              cb::mcbp::sla::getSlowOpThreshold(cb::mcbp::ClientOpcode::Get));

    doc = R"({"version": 1, "get": {"slow": "1 m"}})"_json;
    cb::mcbp::sla::reconfigure(doc);
    EXPECT_EQ(std::chrono::minutes(1),
              cb::mcbp::sla::getSlowOpThreshold(cb::mcbp::ClientOpcode::Get));

    doc = R"({"version": 1, "get": {"slow": "2 minutes"}})"_json;
    cb::mcbp::sla::reconfigure(doc);
    EXPECT_EQ(std::chrono::minutes(2),
              cb::mcbp::sla::getSlowOpThreshold(cb::mcbp::ClientOpcode::Get));

    doc = R"({"version": 1, "get": {"slow": "1 h"}})"_json;
    cb::mcbp::sla::reconfigure(doc);
    EXPECT_EQ(std::chrono::hours(1),
              cb::mcbp::sla::getSlowOpThreshold(cb::mcbp::ClientOpcode::Get));

    doc = R"({"version": 1, "get": {"slow": "2 hours"}})"_json;
    cb::mcbp::sla::reconfigure(doc);
    EXPECT_EQ(std::chrono::hours(2),
              cb::mcbp::sla::getSlowOpThreshold(cb::mcbp::ClientOpcode::Get));
}

TEST(McbpSlaReconfig, ReconfigureFiles) {
    cb::mcbp::sla::reconfigure(std::string{SOURCE_ROOT});

    // We should have applied the files in the following order
    //   etc/couchbase/kv/opcode-attributes.json
    //   etc/couchbase/kv/opcode-attributes.d/override-1.json
    //   etc/couchbase/kv/opcode-attributes.d/override-2.json
    // and we should have ignored the file
    //   etc/couchbase/kv/opcode-attributes.d/override-3.disabled

    // We set a custom value for "delete_bucket" in our system default
    EXPECT_EQ(std::chrono::seconds(10),
              cb::mcbp::sla::getSlowOpThreshold(
                      cb::mcbp::ClientOpcode::DeleteBucket));

    // We override the limit for "get" in override-1.json
    EXPECT_EQ(std::chrono::milliseconds(200),
              cb::mcbp::sla::getSlowOpThreshold(cb::mcbp::ClientOpcode::Get));

    // We override the default value with different values in both
    // override-1, 2 and 3 (the last one shouldn't be read). Verify that we
    // got the one from override-2
    EXPECT_EQ(
            std::chrono::hours(1),
            cb::mcbp::sla::getSlowOpThreshold(cb::mcbp::ClientOpcode::Observe));
}

TEST(McbpSlaReconfig, toJSON) {
    // Verify that we try to print the number as easy to read for
    // a human
    auto doc = R"({"version": 1, "get": {"slow": "1000000 ns"}})"_json;
    cb::mcbp::sla::reconfigure(doc);
    auto json = cb::mcbp::sla::to_json();
    EXPECT_EQ("1 ms", json["GET"]["slow"].get<std::string>());

    // Verify that we don't loose information when trying to
    // make the number easier to read.
    doc = R"({"version": 1, "get": {"slow": "1001 ns"}})"_json;
    cb::mcbp::sla::reconfigure(doc);
    json = cb::mcbp::sla::to_json();
    EXPECT_EQ("1001 ns", json["GET"]["slow"].get<std::string>());
}
