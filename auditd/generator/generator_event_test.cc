/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2018-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "generator_event.h"

#include <folly/portability/GTest.h>
#include <nlohmann/json.hpp>

/// @todo Add extra unit tests to verify that we check for the JSON types

class EventParseTest : public ::testing::Test {
public:
protected:
    void SetUp() override {
        /**
         * This is a legal event identifier we can use to test that the parser
         * picks out the correct fields, and that it detects the errors it
         * should
         */
        const auto* input = R"(
{
  "id": 12345,
  "name": "name",
  "description": "description",
  "sync": true,
  "enabled": true,
  "filtering_permitted": true,
  "mandatory_fields": {
    "timestamp": "",
    "real_userid": {
      "domain": "",
      "user": ""
    }
  },
  "optional_fields": {
    "peername": "",
    "sockname": ""
  }
})";
        json = nlohmann::json::parse(input);
    }

protected:
    nlohmann::json json;
};

/**
 * Verify that the members was set to whatever we had in the input
 * descriptor
 */
TEST_F(EventParseTest, TestCorrectInput) {
    Event event(json);
    EXPECT_EQ(12345, event.id);
    EXPECT_EQ("name", event.name);
    EXPECT_EQ("description", event.description);
    EXPECT_TRUE(event.sync);
    EXPECT_TRUE(event.enabled);
    EXPECT_TRUE(event.filtering_permitted);
    EXPECT_EQ(
            nlohmann::json::parse(
                    R"({"timestamp":"","real_userid":{"domain":"","user":""}})"),
            nlohmann::json::parse(event.mandatory_fields));
    EXPECT_EQ(nlohmann::json::parse(R"({"peername":"","sockname":""})"),
              nlohmann::json::parse(event.optional_fields));
}

/**
 * Verify that we detect that a mandatory field is missing
 */
TEST_F(EventParseTest, MandatoryFields) {
    for (const auto& tag : std::vector<std::string>{{"id",
                                                     "name",
                                                     "description",
                                                     "sync",
                                                     "enabled",
                                                     "mandatory_fields",
                                                     "optional_fields"}}) {
        auto removed = json.at(tag);
        json.erase(tag);
        try {
            Event event(json);
            FAIL() << "Should not be able to construct events without \"" << tag
                   << "\"";
        } catch (const nlohmann::json::exception&) {
        }
        json[tag] = removed;
    }
}

/**
 * Verify that we deal with optional values
 */
TEST_F(EventParseTest, OptionalFields) {
    // "filtering_permitted" is optional, and should be set to false if it
    // is missing
    auto removed = json.at("filtering_permitted");
    json.erase("filtering_permitted");
    Event event(json);
    ASSERT_FALSE(event.filtering_permitted);
}
