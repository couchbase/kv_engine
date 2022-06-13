/*
 *     Copyright 2022-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include <folly/portability/GTest.h>
#include <memcached/stat_group.h>

TEST(StatGroupTest, EnsureAllDocumented) {
    auto& instance = StatsGroupManager::getInstance();
    for (int ii = 0; ii < int(StatGroupId::enum_max); ++ii) {
        const auto* entry = instance.lookup(StatGroupId(ii));
        EXPECT_NE(nullptr, entry) << "Missing entry for StatGroupId: " << ii;
    }
}
