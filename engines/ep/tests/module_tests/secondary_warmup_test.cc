/*
 *     Copyright 2023-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "ep_bucket.h"
#include "evp_store_single_threaded_test.h"
#include "item.h"
#include "test_helpers.h"
#include "vbucket.h"
#include "warmup.h"

#include <folly/portability/GTest.h>

#include <regex>

class SecondaryWarmupTest
    : public SingleThreadedEPBucketTest,
      public ::testing::WithParamInterface<std::tuple<std::string, size_t>> {
public:
    void SetUp() override {
        config_string += generateBackendConfig(std::get<0>(GetParam()));
        config_string += ";item_eviction_policy=full_eviction";
#ifdef EP_USE_MAGMA
        config_string += ";" + magmaRollbackConfig;
#endif
        config_string += ";warmup_min_memory_threshold=0;";
        config_string += "warmup_min_items_threshold=0;";
        config_string += "data_traffic_enabled=false;";
        // Set chunk to 0, this means 1 runNextTask = 1 key.
        config_string += "warmup_secondary_min_memory_threshold=100;";
        // Finally configure from parameter so warmup can stop based on items
        config_string += "warmup_secondary_min_items_threshold=" +
                         std::to_string(std::get<1>(GetParam()));

        SingleThreadedEPBucketTest::SetUp();
        setVBucketStateAndRunPersistTask(vbid, vbucket_state_active);

        ASSERT_TRUE(store_items(4, vbid, makeStoredDocKey("key"), ""));
        flush_vbucket_to_disk(vbid, 4);

        resetEngineAndEnableWarmup();

        auto* kvBucket = engine->getKVBucket();
        ASSERT_TRUE(kvBucket);
        auto& epBucket = getEPBucket();
        // No secondary warmup created ... yet
        ASSERT_FALSE(epBucket.getSecondaryWarmup());
    }

    static std::string PrintToStringParamName(
            const ::testing::TestParamInfo<ParamType>& info) {
        return std::get<0>(info.param) + "_item_perc" +
               std::to_string(std::get<1>(info.param));
    }

    void runPrimaryAndEnableTraffic() {
        auto& readerQueue = *task_executor->getLpTaskQ()[READER_TASK_IDX];
        auto* kvBucket = engine->getKVBucket();
        auto& epBucket = getEPBucket();

        // Keep running the reader queue until primary signals complete.
        while (!kvBucket->getPrimaryWarmup()->isComplete()) {
            // Cannot yet enable traffic and secondary warmup is still null.
            EXPECT_EQ(cb::engine_errc::temporary_failure,
                      engine->handleTrafficControlCmd(
                              *cookie, TrafficControlMode::Enabled));
            ASSERT_FALSE(epBucket.getSecondaryWarmup());
            runNextTask(readerQueue);
        }

        // No items loaded.
        auto vb = kvBucket->getVBucket(vbid);
        EXPECT_EQ(0, engine->getEpStats().warmedUpKeys);
        EXPECT_EQ(0, engine->getEpStats().warmedUpValues);
        EXPECT_EQ(0, engine->getEpStats().warmDups);

        EXPECT_EQ(4, vb->getNumItems());
        EXPECT_EQ(4, vb->getNumNonResidentItems());
        EXPECT_EQ(cb::engine_errc::success,
                  engine->handleTrafficControlCmd(*cookie,
                                                  TrafficControlMode::Enabled));
    }
};

// Run start to finish and check warmup works as expected
TEST_P(SecondaryWarmupTest, GoldenPath) {
    auto* kvBucket = engine->getKVBucket();
    ASSERT_TRUE(kvBucket);
    auto& epBucket = getEPBucket();
    // No secondary has been created
    ASSERT_FALSE(epBucket.getSecondaryWarmup());
    runPrimaryAndEnableTraffic();

    // Secondary warmup now exists
    const auto* secondary = epBucket.getSecondaryWarmup();
    ASSERT_TRUE(secondary);

    const auto* primary = epBucket.getPrimaryWarmup();
    // secondary should of cloned the estimated item count, which is initialised
    // in a warm-up step that is skipped by Secondary.
    EXPECT_EQ(primary->getEstimatedItemCount(),
              secondary->getEstimatedItemCount());

    // Some other checks, primary is done, secondary is not.
    EXPECT_EQ("complete", primary->getThreadStatState());
    EXPECT_EQ("running", secondary->getThreadStatState());
    EXPECT_TRUE(primary->hasReachedThreshold());
    EXPECT_TRUE(primary->isFinishedLoading());
    EXPECT_TRUE(primary->isComplete());

    EXPECT_FALSE(secondary->hasReachedThreshold());
    EXPECT_FALSE(secondary->isFinishedLoading());
    EXPECT_FALSE(secondary->isComplete());

    // This test should not encounter any failures
    EXPECT_FALSE(primary->hasSetVbucketStateFailure());
    EXPECT_FALSE(primary->hasOOMFailure());

    EXPECT_FALSE(secondary->hasSetVbucketStateFailure());
    EXPECT_FALSE(secondary->hasOOMFailure());

    // Now we should be able to step more and find secondary warmup continues
    auto& readerQueue = *task_executor->getLpTaskQ()[READER_TASK_IDX];
    while (!epBucket.getSecondaryWarmup()->isComplete()) {
        runNextTask(readerQueue);
    }

    auto vb = kvBucket->getVBucket(vbid);

    EXPECT_EQ(4, vb->getNumItems());
    // In the case where warmup stops short of 100% it will have loaded 1 extra
    // item. E,g, if 25% is configured, 2 items are loaded. This is because
    // the stopLoading check occurs before each key is loaded, but still stores
    // the currently loading key.
    auto expected = size_t((4 * std::get<1>(GetParam()) / 100.0));
    expected = std::min(size_t(4), expected + 1);

    EXPECT_EQ(expected, engine->getEpStats().warmedUpKeys);
    EXPECT_EQ(expected, engine->getEpStats().warmedUpValues);
    EXPECT_EQ(0, engine->getEpStats().warmDups);

    EXPECT_EQ(4 - expected, vb->getNumNonResidentItems());
}

TEST_P(SecondaryWarmupTest, WritingAndWarming) {
    auto* kvBucket = engine->getKVBucket();
    ASSERT_TRUE(kvBucket);
    auto& epBucket = getEPBucket();
    runPrimaryAndEnableTraffic();
    ASSERT_TRUE(epBucket.getSecondaryWarmup());

    auto& readerQueue = *task_executor->getLpTaskQ()[READER_TASK_IDX];
    const auto* warmup = epBucket.getSecondaryWarmup();

    // Add a hook which is called from the loading callback, yet before Warmup
    // inserts the loaded key. We will write our own key which must not be
    // replaced.
    int keyCount = 0;
    engine->visitWarmupHook = [&keyCount, this]() {
        // Insert the key we're about to load!
        store_item(vbid,
                   makeStoredDocKey("key" + std::to_string(keyCount)),
                   "CorrectValue");
        ++keyCount;
    };

    while (!warmup->isComplete()) {
        runNextTask(readerQueue);
    }

    auto vb = kvBucket->getVBucket(vbid);
    EXPECT_EQ(4, vb->getNumItems());
    // In the case where warmup stops short of 100% it will have loaded 1 extra
    // item. E,g, if 25% is configured, 2 items are loaded. This is because
    // the stopLoading check occurs before each key is loaded, but still stores
    // the currently loading key.
    auto expected = size_t((4 * std::get<1>(GetParam()) / 100.0));
    expected = std::min(size_t(4), expected + 1);
    ASSERT_EQ(expected, keyCount) << "callback count is incorrect";
    EXPECT_EQ(expected, engine->getEpStats().warmedUpKeys);
    EXPECT_EQ(expected, engine->getEpStats().warmedUpValues);
    EXPECT_EQ(expected, engine->getEpStats().warmDups);
    EXPECT_EQ(4 - expected, vb->getNumNonResidentItems());

    // Read back and check that the keys inserted during warmup are the correct
    // version.
    for (size_t ii = 0; ii < expected; ++ii) {
        auto gv = epBucket.get(makeStoredDocKey("key" + std::to_string(ii)),
                               vbid,
                               cookie,
                               NONE);
        ASSERT_EQ(cb::engine_errc::success, gv.getStatus());
        EXPECT_EQ("CorrectValue", gv.item->getValueView());
    }
}

auto testConfig = ::testing::Combine(::testing::Values("persistent_couchdb"
#ifdef EP_USE_MAGMA
                                                       ,
                                                       "persistent_magma"
#endif
                                                       ),
                                     ::testing::Values(100, 50, 25));

// Test that only attempt to create a scan have no need to run in key and value
// variations. Use key only
INSTANTIATE_TEST_SUITE_P(SecondaryWarmupTest,
                         SecondaryWarmupTest,
                         testConfig,
                         SecondaryWarmupTest::PrintToStringParamName);
