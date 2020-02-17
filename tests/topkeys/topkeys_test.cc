/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2015 Couchbase, Inc.
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

#include "daemon/settings.h"
#include "daemon/topkeys.h"
#include <folly/portability/GTest.h>
#include <memory>

class TopKeysTest : public ::testing::Test {
protected:
    void SetUp() {
        Settings::instance().setTopkeysEnabled(true);
        topkeys.reset(new TopKeys(10));
    }

    void testWithNKeys(int numKeys);

    std::unique_ptr<TopKeys> topkeys;
};

static void dump_key(std::string_view,
                     std::string_view,
                     gsl::not_null<const void*> cookie) {
    auto* count = static_cast<size_t*>(const_cast<void*>(cookie.get()));
    (*count)++;
}

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
    topkeys->stats(&count, 0, dump_key);
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
