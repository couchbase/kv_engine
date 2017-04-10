/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2016 Couchbase, Inc
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

/*
 * Unit test for stats
 */

#include "stats_test.h"

#include <gmock/gmock.h>

void StatTest::SetUp() {
    SingleThreadedEPBucketTest::SetUp();
    store->setVBucketState(vbid, vbucket_state_active, false);
}

std::map<std::string, std::string> StatTest::get_stat(const char* statkey) {
    // Define a lambda to use as the ADD_STAT callback. Note we cannot use
    // a capture for the statistics map (as it's a C-style callback), so
    // instead pass via the cookie.
    using StatMap = std::map<std::string, std::string>;
    StatMap stats;
    auto add_stats = [](const char* key,
                        const uint16_t klen,
                        const char* val,
                        const uint32_t vlen,
                        const void* cookie) {
        auto* stats = reinterpret_cast<StatMap*>(const_cast<void*>(cookie));
        std::string k(key, klen);
        std::string v(val, vlen);
        (*stats)[k] = v;
    };

    ENGINE_HANDLE* handle = reinterpret_cast<ENGINE_HANDLE*>(engine.get());
    EXPECT_EQ(ENGINE_SUCCESS,
              engine->get_stats(handle,
                                &stats,
                                statkey,
                                statkey == NULL ? 0 : strlen(statkey),
                                add_stats))
        << "Failed to get stats.";

    return stats;
}

TEST_F(StatTest, vbucket_seqno_stats_test) {
    using namespace testing;
    const std::string vbucket = "vb_" + std::to_string(vbid);
    auto vals = get_stat("vbucket-seqno");

    EXPECT_THAT(vals, UnorderedElementsAre(
            Key(vbucket + ":uuid"),
            Pair(vbucket + ":high_seqno", "0"),
            Pair(vbucket + ":abs_high_seqno", "0"),
            Pair(vbucket + ":last_persisted_seqno", "0"),
            Pair(vbucket + ":purge_seqno", "0"),
            Pair(vbucket + ":last_persisted_snap_start", "0"),
            Pair(vbucket + ":last_persisted_snap_end", "0")));
}
