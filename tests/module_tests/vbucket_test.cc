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

#include "config.h"

#include <gtest/gtest.h>

#include "bgfetcher.h"
#include "item.h"
#include "vbucket.h"

// Dummy time functions (required by Item constructor).
// TODO: Unify with the code in checkpoint_test.cc
static rel_time_t basic_current_time(void) { return 0; }

rel_time_t (*ep_current_time)() = basic_current_time;

time_t ep_real_time() {
    return time(NULL);
}

/**
 * Dummy callback to replace the flusher callback.
 */
class DummyCB: public Callback<uint16_t> {
public:
    DummyCB() {}

    void callback(uint16_t &dummy) { }
};


class VBucketTest : public ::testing::Test {
protected:
    void SetUp() {
        vbucket.reset(new VBucket(0, vbucket_state_active, global_stats,
                                  checkpoint_config, /*kvshard*/nullptr,
                                  /*lastSeqno*/1000, /*lastSnapStart*/0,
                                  /*lastSnapEnd*/0, /*table*/nullptr,
                                  std::make_shared<DummyCB>()));
    }

    void TearDown() {
        vbucket.reset();
    }

    std::unique_ptr<VBucket> vbucket;
    EPStats global_stats;
    CheckpointConfig checkpoint_config;
};

// Measure performance of VBucket::getBGFetchItems - queue and then get
// 10,000 items from the vbucket.
TEST_F(VBucketTest, GetBGFetchItemsPerformance) {
    BgFetcher fetcher(/*store*/nullptr, /*shard*/nullptr, global_stats);

    for (unsigned int ii = 0; ii < 100000; ii++) {
        auto* fetchItem = new VBucketBGFetchItem(/*cookie*/nullptr,
                                                 /*isMeta*/false);
        vbucket->queueBGFetchItem(std::to_string(ii), fetchItem, &fetcher);
    }
    auto items = vbucket->getBGFetchItems();

    // Cleanup.
    for (auto& it : items) {
        for (auto* fetchItem : it.second.bgfetched_list) {
            delete fetchItem;
        }
    }
}


class VBucketEvictionTest : public VBucketTest,
                            public ::testing::WithParamInterface<item_eviction_policy_t> {
};


// Check that counts of items and resident items are as expected when items are
// ejected from the HashTable.
TEST_P(VBucketEvictionTest, EjectionResidentCount) {
    const auto eviction_policy = GetParam();
    ASSERT_EQ(0, this->vbucket->getNumItems(eviction_policy));
    ASSERT_EQ(0, this->vbucket->getNumNonResidentItems(eviction_policy));

    std::string key{"key"};
    Item item{key.c_str(), uint16_t(key.size()), /*flags*/0, /*exp*/0,
              /*data*/nullptr, /*ndata*/0};

    EXPECT_EQ(WAS_CLEAN,
              this->vbucket->ht.set(item, eviction_policy));

    EXPECT_EQ(1, this->vbucket->getNumItems(eviction_policy));
    EXPECT_EQ(0, this->vbucket->getNumNonResidentItems(eviction_policy));

    // TODO-MT: Should acquire lock really (ok given this is currently
    // single-threaded).
    auto* stored_item = this->vbucket->ht.find(key);
    EXPECT_NE(nullptr, stored_item);
    // Need to clear the dirty flag to allow it to be ejected.
    stored_item->markClean();
    EXPECT_TRUE(this->vbucket->ht.unlocked_ejectItem(stored_item,
                                                     eviction_policy));

    // After ejection, should still have 1 item in VBucket, but also have
    // 1 non-resident item.
    EXPECT_EQ(1, this->vbucket->getNumItems(eviction_policy));
    EXPECT_EQ(1, this->vbucket->getNumNonResidentItems(eviction_policy));
}

// Test cases which run in both Full and Value eviction
INSTANTIATE_TEST_CASE_P(FullAndValueEviction,
                        VBucketEvictionTest,
                        ::testing::Values(VALUE_ONLY, FULL_EVICTION),
                        [] (const ::testing::TestParamInfo<item_eviction_policy_t>& info) {
                            if (info.param == VALUE_ONLY) {
                                return "VALUE_ONLY";
                            } else {
                                return "FULL_EVICTION";
                            }
                        });


/* static storage for environment variable set by putenv(). */
static char allow_no_stats_env[] = "ALLOW_NO_STATS_UPDATE=yeah";

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    putenv(allow_no_stats_env);

    HashTable::setDefaultNumBuckets(5);
    HashTable::setDefaultNumLocks(1);

    return RUN_ALL_TESTS();
}
