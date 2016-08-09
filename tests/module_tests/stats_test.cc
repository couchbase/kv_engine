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

#include "evp_store_single_threaded_test.h"
#include <gmock/gmock.h>

static std::map<std::string, std::string> vals;

static void add_stats(const char *key, const uint16_t klen,
                      const char *val, const uint32_t vlen,
                      const void *cookie) {
    (void)cookie;
    std::string k(key, klen);
    std::string v(val, vlen);
    vals[k] = v;
}

class StatTest : public SingleThreadedEPStoreTest {
protected:

    void SetUp() {
        SingleThreadedEPStoreTest::SetUp();
        store->setVBucketState(vbid, vbucket_state_active, false);
    }

    void get_stat(const char *statname, const char *statkey = NULL) {
        vals.clear();
        ENGINE_HANDLE *handle = reinterpret_cast<ENGINE_HANDLE*>(engine.get());
        EXPECT_EQ(ENGINE_SUCCESS, engine->get_stats(handle, NULL, statkey,
                                                       statkey == NULL ? 0 :
                                                               strlen(statkey),
                                                               add_stats))
        << "Failed to get stats.";
    }
};

TEST_F(StatTest, vbucket_seqno_stats_test) {
    using namespace testing;
    const std::string vbucket = "vb_" + std::to_string(vbid);
    get_stat(nullptr, "vbucket-seqno");

    EXPECT_THAT(vals, UnorderedElementsAre(
            Key(vbucket + ":uuid"),
            Pair(vbucket + ":high_seqno", "0"),
            Pair(vbucket + ":abs_high_seqno", "0"),
            Pair(vbucket + ":last_persisted_seqno", "0"),
            Pair(vbucket + ":purge_seqno", "0"),
            Pair(vbucket + ":last_persisted_snap_start", "0"),
            Pair(vbucket + ":last_persisted_snap_end", "0")));
}
