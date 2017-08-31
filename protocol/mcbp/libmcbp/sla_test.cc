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
#include "config.h"

#include <cJSON_utils.h>
#include <gtest/gtest.h>
#include <mcbp/mcbp.h>

TEST(McbpSlaReconfig, MissingVersion) {
    unique_cJSON_ptr doc(cJSON_Parse("{}"));

    try {
        cb::mcbp::sla::reconfigure(*doc);
        FAIL() << "version is a mandatory element";
    } catch (const std::invalid_argument& e) {
        EXPECT_STREQ(
                "cb::mcbp::sla::reconfigure: Missing mandatory element "
                "'version'",
                e.what());
    }
}

TEST(McbpSlaReconfig, IncorrectVersionType) {
    unique_cJSON_ptr doc(cJSON_Parse(R"({"version":true})"));
    try {
        cb::mcbp::sla::reconfigure(*doc);
        FAIL() << "version should be a number";
    } catch (const std::invalid_argument& e) {
        EXPECT_STREQ("cb::mcbp::sla::reconfigure: 'version' should be a number",
                     e.what());
    }
}

TEST(McbpSlaReconfig, UnsupportedVersion) {
    unique_cJSON_ptr doc(cJSON_Parse(R"({"version":2})"));
    try {
        cb::mcbp::sla::reconfigure(*doc);
        FAIL() << "version should be 1";
    } catch (const std::invalid_argument& e) {
        EXPECT_STREQ("cb::mcbp::sla::reconfigure: Unsupported version: 2",
                     e.what());
    }
}

TEST(McbpSlaReconfig, SupportedVersion) {
    unique_cJSON_ptr doc(cJSON_Parse(R"({"version":1})"));
    cb::mcbp::sla::reconfigure(*doc);
}

TEST(McbpSlaReconfig, DefaultEntryNotAnObject) {
    unique_cJSON_ptr doc(cJSON_Parse(
            R"({"version":1,"default": false})"));
    try {
        cb::mcbp::sla::reconfigure(*doc);
        FAIL() << "Default must be an object";
    } catch (const std::invalid_argument& e) {
        EXPECT_STREQ(
                "cb::mcbp::sla::parseThresholdEntry: Entry 'default' is not an "
                "object",
                e.what());
    }
}

TEST(McbpSlaReconfig, DefaultObjectMissingSlow) {
    unique_cJSON_ptr doc(cJSON_Parse(
            R"({"version":1,"default": {}})"));
    try {
        cb::mcbp::sla::reconfigure(*doc);
        FAIL() << "Default must contain 'slow'";
    } catch (const std::invalid_argument& e) {
        EXPECT_STREQ(
                "cb::mcbp::sla::parseThresholdEntry: Entry 'default' does not "
                "contain a mandatory 'slow' entry",
                e.what());
    }
}

TEST(McbpSlaReconfig, LegalDefaultObject) {
    unique_cJSON_ptr doc(cJSON_Parse(
            R"({"version":1,"default": {"slow":500}})"));
    cb::mcbp::sla::reconfigure(*doc);
}

TEST(McbpSlaReconfig, UnknownCommand) {
    unique_cJSON_ptr doc(cJSON_Parse(
            R"({"version":1,"foo": {"slow":500}})"));

    try {
        cb::mcbp::sla::reconfigure(*doc);
        FAIL() << "Should not allow unknown commands";
    } catch (const std::invalid_argument& e) {
        EXPECT_STREQ("cb::mcbp::sla::reconfigure: Unknown command 'foo'",
                     e.what());
    }
}

TEST(McbpSlaReconfig, GetEntryNotAnObject) {
    unique_cJSON_ptr doc(cJSON_Parse(
            R"({"version":1,"get": false})"));
    try {
        cb::mcbp::sla::reconfigure(*doc);
        FAIL() << "Entries must be an object";
    } catch (const std::invalid_argument& e) {
        EXPECT_STREQ(
                "cb::mcbp::sla::parseThresholdEntry: Entry 'get' is not an "
                "object",
                e.what());
    }
}

TEST(McbpSlaReconfig, GetObjectMissingSlow) {
    unique_cJSON_ptr doc(cJSON_Parse(
            R"({"version":1,"get": {}})"));
    try {
        cb::mcbp::sla::reconfigure(*doc);
        FAIL() << "Entries must contain 'slow'";
    } catch (const std::invalid_argument& e) {
        EXPECT_STREQ(
                "cb::mcbp::sla::parseThresholdEntry: Entry 'get' does not "
                "contain a mandatory 'slow' entry",
                e.what());
    }
}

TEST(McbpSlaReconfig, LegalGetObject) {
    unique_cJSON_ptr doc(cJSON_Parse(
            R"({"version":1,"get": {"slow":500}})"));
    cb::mcbp::sla::reconfigure(*doc);
}

// Test all of the allowed permutations of string values to specify a time
// gap
TEST(McbpSlaReconfig, GetParseStringValues) {
    unique_cJSON_ptr doc;

    doc.reset(cJSON_Parse(
            R"({"version":1,"get": {"slow": "1 ns"}})"));
    cb::mcbp::sla::reconfigure(*doc);
    EXPECT_EQ(std::chrono::nanoseconds(1),
              cb::mcbp::sla::getSlowOpThreshold(cb::mcbp::ClientOpcode::Get));

    doc.reset(cJSON_Parse(
            R"({"version":1,"get": {"slow": "2 nanoseconds"}})"));
    cb::mcbp::sla::reconfigure(*doc);
    EXPECT_EQ(std::chrono::nanoseconds(2),
              cb::mcbp::sla::getSlowOpThreshold(cb::mcbp::ClientOpcode::Get));

    doc.reset(cJSON_Parse(
            R"({"version":1,"get": {"slow": "1 us"}})"));
    cb::mcbp::sla::reconfigure(*doc);
    EXPECT_EQ(std::chrono::microseconds(1),
              cb::mcbp::sla::getSlowOpThreshold(cb::mcbp::ClientOpcode::Get));

    doc.reset(cJSON_Parse(
            R"({"version":1,"get": {"slow": "2 microseconds"}})"));
    cb::mcbp::sla::reconfigure(*doc);
    EXPECT_EQ(std::chrono::microseconds(2),
              cb::mcbp::sla::getSlowOpThreshold(cb::mcbp::ClientOpcode::Get));

    doc.reset(cJSON_Parse(
            R"({"version":1,"get": {"slow": "1 ms"}})"));
    cb::mcbp::sla::reconfigure(*doc);
    EXPECT_EQ(std::chrono::milliseconds(1),
              cb::mcbp::sla::getSlowOpThreshold(cb::mcbp::ClientOpcode::Get));

    doc.reset(cJSON_Parse(
            R"({"version":1,"get": {"slow": "2 milliseconds"}})"));
    cb::mcbp::sla::reconfigure(*doc);
    EXPECT_EQ(std::chrono::milliseconds(2),
              cb::mcbp::sla::getSlowOpThreshold(cb::mcbp::ClientOpcode::Get));

    doc.reset(cJSON_Parse(
            R"({"version":1,"get": {"slow": "1 s"}})"));
    cb::mcbp::sla::reconfigure(*doc);
    EXPECT_EQ(std::chrono::seconds(1),
              cb::mcbp::sla::getSlowOpThreshold(cb::mcbp::ClientOpcode::Get));

    doc.reset(cJSON_Parse(
            R"({"version":1,"get": {"slow": "2 seconds"}})"));
    cb::mcbp::sla::reconfigure(*doc);
    EXPECT_EQ(std::chrono::seconds(2),
              cb::mcbp::sla::getSlowOpThreshold(cb::mcbp::ClientOpcode::Get));

    doc.reset(cJSON_Parse(
            R"({"version":1,"get": {"slow": "1 m"}})"));
    cb::mcbp::sla::reconfigure(*doc);
    EXPECT_EQ(std::chrono::minutes(1),
              cb::mcbp::sla::getSlowOpThreshold(cb::mcbp::ClientOpcode::Get));

    doc.reset(cJSON_Parse(
            R"({"version":1,"get": {"slow": "2 minutes"}})"));
    cb::mcbp::sla::reconfigure(*doc);
    EXPECT_EQ(std::chrono::minutes(2),
              cb::mcbp::sla::getSlowOpThreshold(cb::mcbp::ClientOpcode::Get));

    doc.reset(cJSON_Parse(
            R"({"version":1,"get": {"slow": "1 h"}})"));
    cb::mcbp::sla::reconfigure(*doc);
    EXPECT_EQ(std::chrono::hours(1),
              cb::mcbp::sla::getSlowOpThreshold(cb::mcbp::ClientOpcode::Get));

    doc.reset(cJSON_Parse(
            R"({"version":1,"get": {"slow": "2 hours"}})"));
    cb::mcbp::sla::reconfigure(*doc);
    EXPECT_EQ(std::chrono::hours(2),
              cb::mcbp::sla::getSlowOpThreshold(cb::mcbp::ClientOpcode::Get));
}

TEST(McbpSlaReconfig, ReconfigureFiles) {
    cb::mcbp::sla::reconfigure(SOURCE_ROOT);

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
    EXPECT_EQ(std::chrono::hours(1),
              cb::mcbp::sla::getSlowOpThreshold(
                      cb::mcbp::ClientOpcode::SelectBucket));
}
