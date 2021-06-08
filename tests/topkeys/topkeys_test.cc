/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2015-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "daemon/settings.h"
#include "daemon/topkeys.h"
#include <folly/portability/GTest.h>
#include <memory>

class TopKeysTest : public ::testing::Test {
protected:
    void SetUp() override {
        Settings::instance().setTopkeysEnabled(true);
        topkeys = std::make_unique<TopKeys>(10);
    }

    void testWithNKeys(int numKeys);

    std::unique_ptr<TopKeys> topkeys;
};

void TopKeysTest::testWithNKeys(int numKeys) {
    // build list of keys
    std::vector<std::string> keys;
    for (int ii = 0; ii < numKeys; ii++) {
        keys.emplace_back("topkey_test_" + std::to_string(ii));
    }

    // loop inserting keys
    for (int jj = 0; jj < 2000; jj++) {
        for (auto& key : keys) {
            topkeys->updateKey(key.c_str(), key.size(), jj);
        }
    }

    // We should return the lesser of the number of keys used or 80 (because we
    // constructed TopKeys with size 80).
    auto expectedCount = std::min(numKeys, 80);

    // Verify that we return the correct number of top keys.
    size_t count = 0;
    auto dump_key = [&count](std::string_view,
                             std::string_view,
                             const void* ctx) { count++; };
    topkeys->stats(nullptr, 0, dump_key);
    EXPECT_EQ(expectedCount, count);
}

TEST_F(TopKeysTest, Basic) {
    testWithNKeys(160);
}

TEST_F(TopKeysTest, FewerThan80Keys) {
    testWithNKeys(1);
    testWithNKeys(5);
    testWithNKeys(20);
}
