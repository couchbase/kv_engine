/*
 *     Copyright 2023-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "throttle_utilities.h"
#include <folly/portability/GTest.h>
#include <nlohmann/json.hpp>

using namespace cb::throttle;

TEST(SetThrottleLimitPayload, AllDefault) {
    SetThrottleLimitPayload limits = nlohmann::json::parse("{}");
    EXPECT_EQ(std::numeric_limits<std::size_t>::max(), limits.reserved);
    EXPECT_EQ(std::numeric_limits<std::size_t>::max(), limits.hard_limit);
}

TEST(SetThrottleLimitPayload, NumericValues) {
    SetThrottleLimitPayload limits =
            nlohmann::json::parse(R"({"reserved": 1, "hard_limit": 2})");
    EXPECT_EQ(1, limits.reserved);
    EXPECT_EQ(2, limits.hard_limit);
}

TEST(SetThrottleLimitPayload, StringValues) {
    SetThrottleLimitPayload limits = nlohmann::json::parse(
            R"({"reserved": 1, "hard_limit": "unlimited"})");
    EXPECT_EQ(1, limits.reserved);
    EXPECT_EQ(std::numeric_limits<std::size_t>::max(), limits.hard_limit);
}

TEST(SetThrottleLimitPayload, ToJson) {
    nlohmann::json json = SetThrottleLimitPayload();
    EXPECT_EQ(R"({"hard_limit":"unlimited","reserved":"unlimited"})",
              json.dump());
    json = SetThrottleLimitPayload(1, 2);
    EXPECT_EQ(R"({"hard_limit":2,"reserved":1})", json.dump());
}

TEST(SetNodeThrottleLimitPayload, AllDefault) {
    SetNodeThrottleLimitPayload limits = nlohmann::json::parse("{}");
    EXPECT_FALSE(limits.capacity);
    EXPECT_FALSE(limits.default_throttle_reserved_units);
    EXPECT_FALSE(limits.default_throttle_hard_limit);
}

TEST(SetNodeThrottleLimitPayload, Capacity) {
    SetNodeThrottleLimitPayload limits =
            nlohmann::json::parse(R"({"capacity":1})");
    EXPECT_TRUE(limits.capacity);
    EXPECT_EQ(1, limits.capacity.value());
    EXPECT_FALSE(limits.default_throttle_reserved_units);
    EXPECT_FALSE(limits.default_throttle_hard_limit);

    limits = nlohmann::json::parse(R"({"capacity":0})");
    EXPECT_TRUE(limits.capacity);
    EXPECT_EQ(0, limits.capacity.value());

    limits = nlohmann::json::parse(R"({"capacity":"unlimited"})");
    EXPECT_TRUE(limits.capacity);
    EXPECT_EQ(std::numeric_limits<std::size_t>::max(), limits.capacity.value());
}

TEST(SetNodeThrottleLimitPayload, DefaultThrottle) {
    SetNodeThrottleLimitPayload limits =
            nlohmann::json{{"default_throttle_reserved_units", 10},
                           {"default_throttle_hard_limit", 25}};
    EXPECT_FALSE(limits.capacity);
    EXPECT_TRUE(limits.default_throttle_reserved_units);
    EXPECT_TRUE(limits.default_throttle_hard_limit);
    EXPECT_EQ(10, limits.default_throttle_reserved_units.value());
    EXPECT_EQ(25, limits.default_throttle_hard_limit.value());

    // They may be equal
    limits = nlohmann::json{{"default_throttle_reserved_units", 40},
                            {"default_throttle_hard_limit", 40}};
    EXPECT_FALSE(limits.capacity);
    EXPECT_TRUE(limits.default_throttle_reserved_units);
    EXPECT_TRUE(limits.default_throttle_hard_limit);
    EXPECT_EQ(40, limits.default_throttle_reserved_units.value());
    EXPECT_EQ(40, limits.default_throttle_hard_limit.value());

    // They may be string
    limits = nlohmann::json{{"default_throttle_reserved_units", "unlimited"},
                            {"default_throttle_hard_limit", "unlimited"}};
    EXPECT_FALSE(limits.capacity);
    EXPECT_TRUE(limits.default_throttle_reserved_units);
    EXPECT_TRUE(limits.default_throttle_hard_limit);
    EXPECT_EQ(std::numeric_limits<std::size_t>::max(),
              limits.default_throttle_reserved_units.value());
    EXPECT_EQ(std::numeric_limits<std::size_t>::max(),
              limits.default_throttle_hard_limit.value());

    // but reserved cannot exceed hard
    try {
        limits =
                nlohmann::json{{"default_throttle_reserved_units", "unlimited"},
                               {"default_throttle_hard_limit", 100}};
        FAIL() << "reserved must be less or equal to hard limit";
    } catch (const std::exception&) {
    }
}
