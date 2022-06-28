/*
 *     Copyright 2022-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "evp_store_single_threaded_test.h"

#include "../mock/mock_synchronous_ep_engine.h"
#include "checkpoint_manager.h"
#include "ep_engine.h"
#include "kv_bucket.h"
#include "test_helpers.h"
#include "vbucket.h"

#ifdef EP_USE_MAGMA
#include "kvstore/magma-kvstore/magma-kvstore_config.h"
#endif

auto percentOf(size_t val, double percent) {
    return static_cast<size_t>(static_cast<double>(val) * percent);
}

class BucketQuotaChangeTest : public STParameterizedBucketTest {
public:
    void SetUp() override {
        STParameterizedBucketTest::SetUp();
        setVBucketStateAndRunPersistTask(vbid, vbucket_state_active);

        // Watermark percentages are the percentage of the current watermark
        // of the current quota
        initialMemLowWatPercent = engine->getEpStats().mem_low_wat_percent;
        initialMemHighWatPercent = engine->getEpStats().mem_high_wat_percent;

        // Test setup doesn't call KVBucket::initialize (which creates the quota
        // change task) so we have to do that manually here.
        store->createAndScheduleBucketQuotaChangeTask();

        // Ephemeral fail new data does not use an item pager, so don't run it
        // here.
        if (!ephemeralFailNewData()) {
            store->enableItemPager();
        }

        // Drop task wait time to run the next stage instantly so that:
        // 1) we don't have to wait
        // 2) we can run the task and the ItemPager manually and assert the
        //    order in which they run
        engine->getConfiguration().setBucketQuotaChangeTaskPollInterval(0);
    }

    void runQuotaChangeTaskOnce() {
        auto& lpNonioQ = *task_executor->getLpTaskQ()[NONIO_TASK_IDX];
        // run the task.
        runNextTask(lpNonioQ, "Changing bucket quota");
    }

    void runItemPagerTask() {
        auto& lpNonioQ = *task_executor->getLpTaskQ()[NONIO_TASK_IDX];
        // run the task.
        runNextTask(lpNonioQ, "Paging out items.");
    }

    size_t getCurrentBucketQuota() {
        return engine->getEpStats().getMaxDataSize();
    }

    void setBucketQuota(size_t newValue) {
        auto oldQuota = getCurrentBucketQuota();
        std::string msg;
        engine->setFlushParam("max_size", std::to_string(newValue), msg);

        // Changes don't take place until we run the task
        EXPECT_EQ(oldQuota, engine->getEpStats().getMaxDataSize());
    }

    void setBucketQuotaAndRunQuotaChangeTask(size_t newValue) {
        setBucketQuota(newValue);
        runQuotaChangeTaskOnce();
    }

    void setLowWatermark(double percentage) {
        auto newValue = percentOf(getCurrentBucketQuota(), percentage);
        std::string msg;
        engine->setFlushParam("mem_low_wat", std::to_string(newValue), msg);
        initialMemLowWatPercent = percentage;
    }

    void setHighWatermark(double percentage) {
        auto newValue = percentOf(getCurrentBucketQuota(), percentage);
        std::string msg;
        engine->setFlushParam("mem_high_wat", std::to_string(newValue), msg);
        initialMemHighWatPercent = percentage;
    }

    void checkQuota(size_t expected) {
        EXPECT_EQ(expected, engine->getConfiguration().getMaxSize());
        EXPECT_EQ(expected, getCurrentBucketQuota());
    }

    void checkWatermarkValues(size_t quotaValue) {
        EXPECT_EQ(initialMemLowWatPercent,
                  engine->getEpStats().mem_low_wat_percent);
        EXPECT_EQ(percentOf(quotaValue, initialMemLowWatPercent),
                  engine->getConfiguration().getMemLowWat());
        EXPECT_EQ(initialMemHighWatPercent,
                  engine->getEpStats().mem_high_wat_percent);
        EXPECT_EQ(percentOf(quotaValue, initialMemHighWatPercent),
                  engine->getConfiguration().getMemHighWat());
    }

    void checkMaxRunningBackfills(size_t quotaValue) {
        EXPECT_EQ(engine->getDcpConnMap().getMaxRunningBackfillsForQuota(
                          quotaValue),
                  engine->getDcpConnMap().getMaxRunningBackfills());
    }

    void checkStorageEngineQuota(size_t quotaValue) {
#ifdef EP_USE_MAGMA
        if (STParameterizedBucketTest::isMagma()) {
            auto& magmaKVStoreConfig = static_cast<const MagmaKVStoreConfig&>(
                    engine->getKVBucket()->getOneRWUnderlying()->getConfig());
            size_t magmaQuota;
            engine->getKVBucket()->getOneRWUnderlying()->getStat("memory_quota",
                                                                 magmaQuota);
            EXPECT_EQ(
                    percentOf(quotaValue,
                              magmaKVStoreConfig.getMagmaMemQuotaRatio()),
                    magmaQuota * engine->getConfiguration().getMaxNumShards());
        }
#endif
    }

    void checkCheckpointMaxSize(size_t quotaValue) {
        EXPECT_EQ(
                percentOf(
                        quotaValue,
                        engine->getConfiguration().getCheckpointMemoryRatio()) /
                        engine->getCheckpointConfig().getMaxCheckpoints() /
                        store->getVBuckets().getNumAliveVBuckets(),
                engine->getCheckpointConfig().getCheckpointMaxSize());
    }

    void checkBucketQuotaAndRelatedValues(size_t quotaValue) {
        SCOPED_TRACE("");
        checkQuota(quotaValue);
        checkWatermarkValues(quotaValue);
        checkMaxRunningBackfills(quotaValue);
        checkStorageEngineQuota(quotaValue);
        checkCheckpointMaxSize(quotaValue);
    }

    void testQuotaChangeUp() {
        SCOPED_TRACE("");
        auto currentQuota = getCurrentBucketQuota();

        {
            SCOPED_TRACE("");
            checkBucketQuotaAndRelatedValues(currentQuota);
        }

        auto newQuota = currentQuota * 2;
        setBucketQuotaAndRunQuotaChangeTask(newQuota);

        {
            SCOPED_TRACE("");
            checkBucketQuotaAndRelatedValues(newQuota);
        }
    }

    void testQuotaChangeDown() {
        SCOPED_TRACE("");
        auto currentQuota = getCurrentBucketQuota();

        {
            SCOPED_TRACE("");
            checkBucketQuotaAndRelatedValues(currentQuota);
        }

        auto newQuota = currentQuota / 2;
        setBucketQuotaAndRunQuotaChangeTask(newQuota);

        {
            SCOPED_TRACE("");
            checkBucketQuotaAndRelatedValues(newQuota);
        }
    }

    void setUpQuotaChangeDownHighMemoryUsageTest() {
        SCOPED_TRACE("");
        // Drop our initial quota values because we're going to create a "large"
        // item to inflate memory usage for the purposes of this test and we
        // don't want that item to have to be too large (allocations aren't
        // fast).
        engine->getConfiguration().setMaxSize(10 * 1024 * 1024);
        engine->setMaxDataSize(10 * 1024 * 1024);

        // We need our item to be a certain size (and hard-coding some size is
        // brittle) so we'll calculate an appropriate value. Our HWM defaults to
        // 85% of the Bucket quota and the new mutation threshold is 93% by
        // default. We'll target 90% memory usage when we store this item to sit
        // between the two. We're going to reduce the quota by half and it's the
        // post-change values that we care about so add a further multiple of
        // 0.5.
        ASSERT_LT(0.9, engine->getConfiguration().getMutationMemThreshold());
        ASSERT_GT(0.9, engine->getEpStats().mem_high_wat_percent);
        auto valueToHit = (0.9 * 0.5 * getCurrentBucketQuota());
        auto size =
                valueToHit - engine->getEpStats().getPreciseTotalMemoryUsed();

        auto key = makeStoredDocKey("key");
        store_item(vbid, StoredDocKey{key}, std::string(size, 'x'), 0);
        // Flushing the item ensure that it's in the storage engine (which we
        // care about if this is a magma test).
        flushVBucketToDiskIfPersistent(vbid, 1);

        // Force create a new Checkpoint and run the checkpoint destroyer task
        // to make sure the CheckpointManager isn't referencing the item. We
        // want the ItemPager to be able to evict the item and for memory to
        // come down when it does so.
        store->getVBucket(vbid)->checkpointManager->createNewCheckpoint();
        runCheckpointDestroyer(vbid);

        auto oldQuota = getCurrentBucketQuota();

        {
            SCOPED_TRACE("");
            checkBucketQuotaAndRelatedValues(oldQuota);
        }

        auto newQuota = oldQuota / 2;
        setBucketQuotaAndRunQuotaChangeTask(newQuota);

        // The BucketQuotaChangeTask will have changed watermark values,
        // backfills, and storage engine quota, before it changes the actual
        // quota, check that those values have been updated now (and that quota
        // is still the same).
        EXPECT_EQ(percentOf(newQuota, initialMemLowWatPercent),
                  engine->getConfiguration().getMemLowWat());

        EXPECT_EQ(percentOf(newQuota, initialMemHighWatPercent),
                  engine->getConfiguration().getMemHighWat());

        checkMaxRunningBackfills(newQuota);
        checkStorageEngineQuota(newQuota);

        checkQuota(oldQuota);
    }

    double initialMemLowWatPercent;
    double initialMemHighWatPercent;
};

TEST_P(BucketQuotaChangeTest, QuotaChangeEqual) {
    SCOPED_TRACE("");
    auto currentQuota = getCurrentBucketQuota();

    {
        SCOPED_TRACE("");
        checkBucketQuotaAndRelatedValues(currentQuota);
    }

    auto newQuota = currentQuota;
    setBucketQuotaAndRunQuotaChangeTask(newQuota);

    {
        SCOPED_TRACE("");
        checkBucketQuotaAndRelatedValues(newQuota);
    }
}

TEST_P(BucketQuotaChangeTest, QuotaChangeDown) {
    SCOPED_TRACE("");
    testQuotaChangeDown();
}

TEST_P(BucketQuotaChangeTest, QuotaChangeUp) {
    SCOPED_TRACE("");
    testQuotaChangeUp();
}

TEST_P(BucketQuotaChangeTest, QuotaChangeUpNonDefaultWatermarks) {
    SCOPED_TRACE("");
    setLowWatermark(0.5);
    setHighWatermark(0.6);
    testQuotaChangeUp();
}

TEST_P(BucketQuotaChangeTest, QuotaChangeDownNonDefaultWatermarks) {
    SCOPED_TRACE("");
    setLowWatermark(0.5);
    setHighWatermark(0.6);
    testQuotaChangeDown();
}

TEST_P(BucketQuotaChangeTest, QuotaChangeDownMemoryUsageHigh) {
    SCOPED_TRACE("");
    setUpQuotaChangeDownHighMemoryUsageTest();

    auto oldQuota = engine->getEpStats().getMaxDataSize();
    auto key = makeStoredDocKey("key");
    auto newQuota = oldQuota / 2;

    if (ephemeralFailNewData()) {
        // Ephemeral fail new data can't recover memory via item paging (which
        // this test relies on) so just run the task again, make sure the quota
        // isn't changing, and end the test early.
        runQuotaChangeTaskOnce();
        EXPECT_EQ(oldQuota, getCurrentBucketQuota());
        return;
    }

    auto diffKey = makeStoredDocKey("diffKey");
    store_item(vbid, diffKey, "value");
    flushVBucketToDiskIfPersistent(vbid, 1);

    // Poke the item and the pager setting to ensure that we will evict it
    store->getVBucket(vbid)
            ->ht.findOnlyCommitted(key)
            .storedValue->setFreqCounterValue(0);
    engine->getConfiguration().setItemEvictionAgePercentage(0);
    engine->getConfiguration().setItemEvictionFreqCounterAgeThreshold(255);

    // ItemPager will have been woken by the BucketQuotaChangeTask after it sets
    // the new watermark values. ItemPager is a multi-phase task though and it
    // will schedule the next stages now (which actually do the eviction).
    auto& lpNonioQ = *task_executor->getLpTaskQ()[NONIO_TASK_IDX];
    runNextTask(lpNonioQ, "Paging out items.");

    // Quota task is up next because it snoozes for the configured duration
    // (wakes up immediately for test purposes) after setting the watermarks
    // if it can't immediately change the quota. It will still find that the
    // memory usage is too high and won't change the quota down yet.
    runQuotaChangeTaskOnce();
    EXPECT_NE(newQuota, engine->getEpStats().getMaxDataSize());

    // Now the two ItemPager tasks can run
    runNextTask(lpNonioQ, "Item pager no vbucket assigned");
    runNextTask(lpNonioQ, "Item pager no vbucket assigned");

    if (ephemeral()) {
        // Item is deleted rather than evicted by the pager for ephemeral
        ASSERT_EQ(1, store->getVBucket(vbid)->getNumInMemoryDeletes());
    } else {
        EXPECT_LT(0, store->getVBucket(vbid)->ht.getNumEjects());
    }

    // Now the BucketQuotaChangeTask can _probably_ change the quota value.
    // For the case of a magma Bucket though it may still be recovering memory
    // in a background thread (which we don't control) so we'll allow magma
    // Buckets a bit of time to reduce memory usage before checking our new
    // values. Anecdotally, this runs in ~40ms on my M1 MacbookPro.
    runQuotaChangeTaskOnce();

    if (STParameterizedBucketTest::isMagma()) {
        // Allow magma 10 seconds to reduce memory.
        auto timeout =
                std::chrono::steady_clock::now() + std::chrono::seconds(10);
        while (engine->getEpStats().getMaxDataSize() != newQuota) {
            if (timeout < std::chrono::steady_clock::now()) {
                // We waited too long for magma to reduce memory usage. Either
                // the test is racey and we need to up the timeout, or magma is
                // not recovering memory for some reason. Fail now for some
                // human intervention rather than hanging the test forever.
                FAIL() << "Magma took too long to reduce memory usage";
            }

            std::this_thread::yield();

            // The BucketQuotaChangeTask is going to interleave with the
            // ItemPager (which reschedules itself if memory usage is still
            // above the HWM after a full iteration). As a result, we can't
            // check the task that ran.
            auto& lpNonioQ = *task_executor->getLpTaskQ()[NONIO_TASK_IDX];
            runNextTask(lpNonioQ);
        }
    }

    // Finally, we can check that the new quota values have applied
    // successfully.
    {
        SCOPED_TRACE("");
        checkBucketQuotaAndRelatedValues(newQuota);
    }
}

TEST_P(BucketQuotaChangeTest, HandleIdenticalQuotaChange) {
    auto currentQuota = getCurrentBucketQuota();

    {
        SCOPED_TRACE("");
        checkBucketQuotaAndRelatedValues(currentQuota);
    }

    auto newQuota = currentQuota / 2;
    setBucketQuotaAndRunQuotaChangeTask(newQuota);

    checkWatermarkValues(newQuota);

    // Should extra change now hit the task?
    setBucketQuota(newQuota);

    {
        SCOPED_TRACE("");
        checkBucketQuotaAndRelatedValues(newQuota);
    }

    runQuotaChangeTaskOnce();
    {
        SCOPED_TRACE("");
        checkBucketQuotaAndRelatedValues(newQuota);
    }
}

TEST_P(BucketQuotaChangeTest, HandleQuotaChangeCancel) {
    auto currentQuota = getCurrentBucketQuota();

    setUpQuotaChangeDownHighMemoryUsageTest();

    // 1 run of the ItemPager doesn't actually free up any memory
    if (!ephemeralFailNewData()) {
        runItemPagerTask();
    }

    setBucketQuotaAndRunQuotaChangeTask(currentQuota);

    {
        SCOPED_TRACE("");
        checkBucketQuotaAndRelatedValues(currentQuota);
    }
}

TEST_P(BucketQuotaChangeTest, HandleQuotaIncreaseDuringQuotaDecreaseChange) {
    auto currentQuota = getCurrentBucketQuota();
    auto newQuota = currentQuota * 2;

    setUpQuotaChangeDownHighMemoryUsageTest();

    // 1 run of the ItemPager doesn't actually free up any memory
    if (!ephemeralFailNewData()) {
        runItemPagerTask();
    }

    setBucketQuotaAndRunQuotaChangeTask(newQuota);

    {
        SCOPED_TRACE("");
        checkBucketQuotaAndRelatedValues(newQuota);
    }
}

INSTANTIATE_TEST_SUITE_P(EphemeralOrPersistent,
                         BucketQuotaChangeTest,
                         STParameterizedBucketTest::allConfigValuesNoNexus(),
                         STParameterizedBucketTest::PrintToStringParamName);
