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

#include "checkpoint_manager.h"
#include "dcp/consumer.h"
#include "dcp/flow-control-manager.h"
#include "ep_engine.h"
#include "item.h"
#include "kv_bucket.h"
#include "kvstore/kvstore_iface.h"
#include "test_helpers.h"
#include "vbucket.h"
#include <utilities/math_utilities.h>

#include "../mock/mock_synchronous_ep_engine.h"

#ifdef EP_USE_MAGMA
#include "kvstore/magma-kvstore/magma-kvstore_config.h"
#endif

class BucketQuotaChangeTest : public STParameterizedBucketTest {
public:
    void SetUp() override {
        // Magma occasionally blows the quota during the test if it isn't quite
        // fast enough to free memory as it has extra copies of the item that we
        // flush lying around. Setting memory optimized writes to true should
        // prevent copies to obey quota limits.
        config_string += ";magma_enable_memory_optimized_writes=true";
        // Set an arbitrarily long item pager sleep time (100s) - we do want
        // it to be scheduled (apart from ephemeralFailNewData); but don't want
        // it to run before quota setting tasks (on CV machines we have seen
        // Magma take longer than the default 5s interval to initialise,
        // causing ItemPager to run before we expect it to).
        config_string += ";pager_sleep_time_ms=100000";

        STParameterizedBucketTest::SetUp();
        setVBucketStateAndRunPersistTask(vbid, vbucket_state_active);

        // Watermark percentages are the percentage of the current watermark
        // of the current quota
        initialMemLowWatPercent =
                engine->getConfiguration().getMemLowWatPercent();
        initialMemHighWatPercent =
                engine->getConfiguration().getMemHighWatPercent();

        // Save original checkpoint mem-recovery ratios.
        // They are set to temp values during the mem-recovery phase. Then at
        // quota-reduction completion they are expected to be reset to their
        // original values.
        initialCkptLowerMark = store->getCheckpointMemoryRecoveryLowerMark();
        initialCkptUpperMark = store->getCheckpointMemoryRecoveryUpperMark();

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
        auto& config = engine->getConfiguration();
        config.setBucketQuotaChangeTaskPollInterval(0);

        // Add a consumer. We use it to verify that DCP buffers are correctly
        // resized at bucket quota changes.
        ASSERT_EQ(0, engine->getDcpFlowControlManager().getNumConsumers());

        auto& connMap = engine->getDcpConnMap();
        consumer = connMap.newConsumer(*cookie, "connection", "consumer");
        ASSERT_TRUE(consumer->isFlowControlEnabled());
        ASSERT_EQ(1, engine->getDcpFlowControlManager().getNumConsumers());
        EXPECT_EQ(config.getMaxSize() * config.getDcpConsumerBufferRatio(),
                  consumer->getFlowControlBufSize());
    }

    void runQuotaChangeTaskOnce() {
        auto& lpNonioQ = *task_executor->getLpTaskQ(TaskType::NonIO);
        // run the task.
        runNextTask(lpNonioQ, "Changing bucket quota");
    }

    void runItemPagerTask() {
        auto& lpNonioQ = *task_executor->getLpTaskQ(TaskType::NonIO);
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
        std::string msg;
        engine->setFlushParam(
                "mem_low_wat_percent", std::to_string(percentage), msg);
        initialMemLowWatPercent = percentage;
    }

    void setHighWatermark(double percentage) {
        std::string msg;
        engine->setFlushParam(
                "mem_high_wat_percent", std::to_string(percentage), msg);
        initialMemHighWatPercent = percentage;
    }

    void checkQuota(size_t expected) {
        EXPECT_EQ(expected, engine->getConfiguration().getMaxSize());
        EXPECT_EQ(expected, getCurrentBucketQuota());
    }

    void checkWatermarkValues(size_t quotaValue) {
        EXPECT_NEAR(initialMemLowWatPercent,
                    engine->getConfiguration().getMemLowWatPercent(),
                    1E-5);
        EXPECT_NEAR(cb::fractionOf(quotaValue, initialMemLowWatPercent),
                    engine->getEpStats().mem_low_wat,
                    16);
        EXPECT_NEAR(initialMemHighWatPercent,
                    engine->getConfiguration().getMemHighWatPercent(),
                    1E-5);
        EXPECT_NEAR(cb::fractionOf(quotaValue, initialMemHighWatPercent),
                    engine->getEpStats().mem_high_wat,
                    16);
    }

    void checkMaxRunningBackfills(size_t quotaValue) {
        EXPECT_EQ(engine->getKVBucket()
                          ->getKVStoreScanTracker()
                          .getMaxRunningScansForQuota(quotaValue, 0.8)
                          .first,
                  engine->getKVBucket()
                          ->getKVStoreScanTracker()
                          .getMaxRunningBackfills());
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
                    cb::fractionOf(quotaValue,
                                   magmaKVStoreConfig.getMagmaMemQuotaRatio()),
                    magmaQuota * engine->getConfiguration().getMaxNumShards());
        }
#endif
    }

    void checkCheckpointMaxSize(size_t quotaValue) {
        const auto& ckptConfig = engine->getCheckpointConfig();
        const size_t expected =
                quotaValue *
                engine->getConfiguration().getCheckpointMemoryRatio() /
                ckptConfig.getMaxCheckpoints() /
                store->getVBuckets().getNumAliveVBuckets();
        EXPECT_EQ(expected, ckptConfig.getCheckpointMaxSize());
    }

    void checkDcpConsumerBuffer(size_t quotaValue) const {
        ASSERT_EQ(1, engine->getDcpFlowControlManager().getNumConsumers());
        const auto& config = engine->getConfiguration();
        const size_t numConsumers =
                engine->getDcpFlowControlManager().getNumConsumers();
        const size_t expectedSize =
                quotaValue * config.getDcpConsumerBufferRatio() / numConsumers;
        EXPECT_EQ(expectedSize, consumer->getFlowControlBufSize());
    }

    void checkBucketQuotaAndRelatedValues(size_t quotaValue) {
        CB_SCOPED_TRACE("");
        checkQuota(quotaValue);
        checkWatermarkValues(quotaValue);
        checkMaxRunningBackfills(quotaValue);
        checkStorageEngineQuota(quotaValue);
        checkCheckpointMaxSize(quotaValue);
        checkDcpConsumerBuffer(quotaValue);
    }

    void checkCkptMarksResetToInitialValues() const {
        EXPECT_EQ(initialCkptLowerMark,
                  store->getCheckpointMemoryRecoveryLowerMark());
        EXPECT_EQ(initialCkptUpperMark,
                  store->getCheckpointMemoryRecoveryUpperMark());
    }

    void testQuotaChangeUp() {
        CB_SCOPED_TRACE("");
        auto currentQuota = getCurrentBucketQuota();

        {
            CB_SCOPED_TRACE("");
            checkBucketQuotaAndRelatedValues(currentQuota);
        }

        auto newQuota = currentQuota * 2;
        setBucketQuotaAndRunQuotaChangeTask(newQuota);

        {
            CB_SCOPED_TRACE("");
            checkBucketQuotaAndRelatedValues(newQuota);
        }
    }

    void testQuotaChangeDown() {
        CB_SCOPED_TRACE("");
        auto currentQuota = getCurrentBucketQuota();

        {
            CB_SCOPED_TRACE("");
            checkBucketQuotaAndRelatedValues(currentQuota);
        }

        auto newQuota = currentQuota / 2;
        setBucketQuotaAndRunQuotaChangeTask(newQuota);

        {
            CB_SCOPED_TRACE("");
            checkBucketQuotaAndRelatedValues(newQuota);
        }
    }

    void setUpQuotaChangeDownHighMemoryUsageTest() {
        CB_SCOPED_TRACE("");
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
        ASSERT_LT(0.9, engine->getConfiguration().getMutationMemRatio());
        ASSERT_GT(0.9, engine->getConfiguration().getMemHighWatPercent());
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
            CB_SCOPED_TRACE("");
            checkBucketQuotaAndRelatedValues(oldQuota);
        }

        auto newQuota = oldQuota / 2;
        setBucketQuotaAndRunQuotaChangeTask(newQuota);

        // The BucketQuotaChangeTask will have changed watermark values,
        // backfills, and storage engine quota, before it changes the actual
        // quota, check that those values have been updated now (and that quota
        // is still the same).
        EXPECT_EQ(cb::fractionOf(newQuota, initialMemLowWatPercent),
                  engine->getEpStats().mem_low_wat);

        EXPECT_EQ(cb::fractionOf(newQuota, initialMemHighWatPercent),
                  engine->getEpStats().mem_high_wat);

        checkMaxRunningBackfills(newQuota);
        checkStorageEngineQuota(newQuota);

        checkQuota(oldQuota);

        // Checkpoint mem-recovery marks changed to temp ratios
        const float changeRatio = static_cast<float>(newQuota) / oldQuota;
        ASSERT_GT(changeRatio, 0.0f);
        EXPECT_EQ(initialCkptLowerMark * changeRatio,
                  store->getCheckpointMemoryRecoveryLowerMark());
        EXPECT_EQ(initialCkptUpperMark * changeRatio,
                  store->getCheckpointMemoryRecoveryUpperMark());
    }

    double initialMemLowWatPercent;
    double initialMemHighWatPercent;

    float initialCkptLowerMark;
    float initialCkptUpperMark;

    // Used for test DCP Consumers buffers resizing at bucket quota changes.
    // Raw ptr, owned by the engine for proper cleanup at test tear-down.
    DcpConsumer* consumer;
};

TEST_P(BucketQuotaChangeTest, QuotaChangeEqual) {
    CB_SCOPED_TRACE("");
    auto currentQuota = getCurrentBucketQuota();

    {
        CB_SCOPED_TRACE("");
        checkBucketQuotaAndRelatedValues(currentQuota);
    }

    auto newQuota = currentQuota;
    setBucketQuotaAndRunQuotaChangeTask(newQuota);

    {
        CB_SCOPED_TRACE("");
        checkBucketQuotaAndRelatedValues(newQuota);
    }
}

TEST_P(BucketQuotaChangeTest, QuotaChangeDown) {
    CB_SCOPED_TRACE("");
    testQuotaChangeDown();
}

TEST_P(BucketQuotaChangeTest, QuotaChangeUp) {
    CB_SCOPED_TRACE("");
    testQuotaChangeUp();
}

TEST_P(BucketQuotaChangeTest, QuotaChangeUpNonDefaultWatermarks) {
    CB_SCOPED_TRACE("");
    setLowWatermark(0.5);
    setHighWatermark(0.6);
    testQuotaChangeUp();
}

TEST_P(BucketQuotaChangeTest, QuotaChangeDownNonDefaultWatermarks) {
    CB_SCOPED_TRACE("");
    setLowWatermark(0.5);
    setHighWatermark(0.6);
    testQuotaChangeDown();
}

TEST_P(BucketQuotaChangeTest, QuotaChangeDownMemoryUsageHigh) {
    CB_SCOPED_TRACE("");
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

    // ItemPager will have been woken by the BucketQuotaChangeTask after it sets
    // the new watermark values. ItemPager is a multi-phase task though and it
    // will schedule the next stages now (which actually do the eviction).
    auto& lpNonioQ = *task_executor->getLpTaskQ(TaskType::NonIO);
    runNextTask(lpNonioQ, "Paging out items.");

    // Quota task is up next because it snoozes for the configured duration
    // (wakes up immediately for test purposes) after setting the watermarks
    // if it can't immediately change the quota. It will still find that the
    // memory usage is too high and won't change the quota down yet.
    runQuotaChangeTaskOnce();
    EXPECT_NE(newQuota, engine->getEpStats().getMaxDataSize());

    // Now the ItemPager visitor task can run.
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
            auto& lpNonioQ = *task_executor->getLpTaskQ(TaskType::NonIO);
            runNextTask(lpNonioQ);
        }
    }

    // Finally, we can check that the new quota values have applied
    // successfully.
    {
        CB_SCOPED_TRACE("");
        checkBucketQuotaAndRelatedValues(newQuota);
        checkCkptMarksResetToInitialValues();
    }
}

TEST_P(BucketQuotaChangeTest, HandleIdenticalQuotaChange) {
    auto currentQuota = getCurrentBucketQuota();

    {
        CB_SCOPED_TRACE("");
        checkBucketQuotaAndRelatedValues(currentQuota);
    }

    auto newQuota = currentQuota / 2;
    setBucketQuotaAndRunQuotaChangeTask(newQuota);

    checkWatermarkValues(newQuota);

    // Should extra change now hit the task?
    setBucketQuota(newQuota);

    {
        CB_SCOPED_TRACE("");
        checkBucketQuotaAndRelatedValues(newQuota);
    }

    runQuotaChangeTaskOnce();
    {
        CB_SCOPED_TRACE("");
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
        CB_SCOPED_TRACE("");
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
        CB_SCOPED_TRACE("");
        checkBucketQuotaAndRelatedValues(newQuota);
    }
}

INSTANTIATE_TEST_SUITE_P(EphemeralOrPersistent,
                         BucketQuotaChangeTest,
                         STParameterizedBucketTest::allConfigValuesNoNexus(),
                         STParameterizedBucketTest::PrintToStringParamName);
