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

TEST(ThrottleLimitPayload, AllDefault) {
    SetThrottleLimitPayload limits = nlohmann::json::parse("{}");
    EXPECT_EQ(std::numeric_limits<std::size_t>::max(), limits.reserved);
    EXPECT_EQ(std::numeric_limits<std::size_t>::max(), limits.hard_limit);
}

TEST(ThrottleLimitPayload, NumericValues) {
    SetThrottleLimitPayload limits =
            nlohmann::json::parse(R"({"reserved": 1, "hard_limit": 2})");
    EXPECT_EQ(1, limits.reserved);
    EXPECT_EQ(2, limits.hard_limit);
}

TEST(ThrottleLimitPayload, StringValues) {
    SetThrottleLimitPayload limits = nlohmann::json::parse(
            R"({"reserved": 1, "hard_limit": "unlimited"})");
    EXPECT_EQ(1, limits.reserved);
    EXPECT_EQ(std::numeric_limits<std::size_t>::max(), limits.hard_limit);
}

TEST(ThrottleLimitPayload, ToJson) {
    nlohmann::json json = SetThrottleLimitPayload();
    EXPECT_EQ(R"({"hard_limit":"unlimited","reserved":"unlimited"})",
              json.dump());
    json = SetThrottleLimitPayload(1, 2);
    EXPECT_EQ(R"({"hard_limit":2,"reserved":1})", json.dump());
}
