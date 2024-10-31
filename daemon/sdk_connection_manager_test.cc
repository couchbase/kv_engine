/*
 *     Copyright 2024-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#include "sdk_connection_manager.h"

#include <folly/portability/GTest.h>

TEST(SdkConnectionManagerTest, Wraps) {
    auto& instance = SdkConnectionManager::instance();
    const auto Max = SdkConnectionManager::MaximumTrackedSdk;

    for (std::size_t ii = 0; ii < Max; ++ii) {
        instance.registerSdk(std::to_string(ii));
    }
    auto sdks = instance.getConnectedSdks();
    EXPECT_EQ(sdks.size(), Max);
    for (std::size_t ii = 0; ii < Max; ++ii) {
        EXPECT_EQ(1, sdks[std::to_string(ii)]);
    }

    instance.registerSdk("1");
    sdks = instance.getConnectedSdks();
    EXPECT_EQ(sdks.size(), Max);
    EXPECT_EQ(2, sdks["1"]);

    instance.registerSdk("evict-one");
    sdks = instance.getConnectedSdks();
    EXPECT_EQ(sdks.size(), Max);
    // The first entry should be evicted
    EXPECT_EQ(sdks.end(), sdks.find("0"));
}