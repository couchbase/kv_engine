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
#include "daemon/topkeys.h"

#include <gtest/gtest.h>
#include <memory>


class TopKeysTest : public ::testing::Test {
protected:
    void SetUp() {
        topkeys.reset(new TopKeys(10));
    }

    std::unique_ptr<TopKeys> topkeys;
};

static void dump_key(const char* key, const uint16_t klen,
                     const char* val, const uint32_t vlen,
                     const void* cookie) {
    size_t* count = static_cast<size_t*>(const_cast<void*>(cookie));
    (*count)++;
}

TEST_F(TopKeysTest, Basic) {
    // build list of keys
    std::vector<std::string> keys;
    for (int ii = 0; ii < 160; ii++) {
        keys.emplace_back("topkey_test_" + std::to_string(ii));
    }

    // loop inserting keys
    for (int jj = 0; jj < 20000; jj++) {
        for (auto& key : keys) {
            topkeys->updateKey(key.c_str(), key.size(), jj);
        }
    }

    // Verify we hit all shards inside TopKeys
    size_t count = 0;
    topkeys->stats(&count, 0, dump_key);
    EXPECT_EQ(80, count);
}
