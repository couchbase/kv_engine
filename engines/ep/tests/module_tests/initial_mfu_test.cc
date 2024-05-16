/*
 *     Copyright 2023-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "../mock/mock_synchronous_ep_engine.h"
#include "evp_store_single_threaded_test.h"
#include "initial_mfu_task.h"
#include "test_helpers.h"
#include "vbucket.h"
#include <item.h>

class InitialMFUTest : public STParameterizedBucketTest {
public:
    static config::Config configValues() {
        return STParameterizedBucketTest::couchstoreConfigValues() *
               config::Config{{"item_eviction_strategy",
                               {"upfront_mfu_only", "learning_age_and_mfu"}}};
    }

    void SetUp() override {
        STParameterizedBucketTest::SetUp();
        initializeInitialMfuUpdater();
    }

    bool isMfuOnlyEviction() const {
        return engine->getConfiguration().getItemEvictionStrategy() ==
               "upfront_mfu_only";
    }

    std::chrono::duration<double> getUpdateInterval() const {
        return std::chrono::duration<double>(
                engine->getConfiguration()
                        .getItemEvictionInitialMfuUpdateInterval());
    }
};

TEST_P(InitialMFUTest, TaskIsInitiallyScheduled) {
    auto task = getInitialMfuUpdaterTask();
    EXPECT_LE(task->getWaketime(),
              std::chrono::steady_clock::now() + getUpdateInterval());
}

TEST_P(InitialMFUTest, TaskRunsPeriodicallyForMfuEvictionOnly) {
    auto task = getInitialMfuUpdaterTask();
    // Run the task synchronously and check the wake time that was set.
    task->run();
    if (isMfuOnlyEviction()) {
        EXPECT_LE(task->getWaketime(),
                  std::chrono::steady_clock::now() + getUpdateInterval());
    } else {
        EXPECT_EQ(task->getWaketime(),
                  std::chrono::steady_clock::time_point::max());
    }
}

TEST_P(InitialMFUTest, TaskRunsAfterEvictionChange) {
    auto task = getInitialMfuUpdaterTask();
    ExecutorPool::get()->snooze(task->getId(), INT_MAX);
    ASSERT_EQ(task->getWaketime(),
              std::chrono::steady_clock::time_point::max());

    engine->getConfiguration().setItemEvictionStrategy("upfront_mfu_only");
    EXPECT_LE(task->getWaketime(),
              std::chrono::steady_clock::now() + getUpdateInterval());
}

/**
 * Initial MFU is set based on MFU histograms.
 */
TEST_P(InitialMFUTest, InitialMfuUpdatedCorrectly) {
    setVBucketState(vbid, vbucket_state_active);

    auto item = make_item(vbid, makeStoredDocKey("1"), "");
    ASSERT_EQ(cb::engine_errc::success, store->set(item, cookie));
    store->getVBucket(vbid)->ht.findItem(item).storedValue->setFreqCounterValue(
            100);

    flushVBucketToDiskIfPersistent(vbid);
    getInitialMfuUpdaterTask()->run();

    if (isMfuOnlyEviction()) {
        EXPECT_EQ(101, store->getInitialMFU());
    } else {
        EXPECT_EQ(Item::initialFreqCount, store->getInitialMFU());
    }
}

/**
 * Ensure that the initial MFU is updated correctly when the eviction strategy
 * changes from upfront -> learning.
 */
TEST_P(InitialMFUTest, SetsToInitialValueWhenTransitioningToLearning) {
    if (!isMfuOnlyEviction()) {
        GTEST_SKIP();
    }

    setVBucketState(vbid, vbucket_state_active);

    auto item = make_item(vbid, makeStoredDocKey("1"), "");
    ASSERT_EQ(cb::engine_errc::success, store->set(item, cookie));
    store->getVBucket(vbid)->ht.findItem(item).storedValue->setFreqCounterValue(
            100);

    flushVBucketToDiskIfPersistent(vbid);
    getInitialMfuUpdaterTask()->run();

    ASSERT_EQ(101, store->getInitialMFU());

    engine->getConfiguration().setItemEvictionStrategy("learning_age_and_mfu");

    auto& lpNonioQ = *task_executor->getLpTaskQ(TaskType::NonIO);
    runNextTask(lpNonioQ, "Initial Item MFU updater");

    EXPECT_EQ(Item::initialFreqCount, store->getInitialMFU());
}

/**
 * Initial MFU should not be < Item::initialFreqCount.
 */
TEST_P(InitialMFUTest, InitialMfuMinimumValue) {
    setVBucketState(vbid, vbucket_state_active);

    auto item = make_item(vbid, makeStoredDocKey("1"), "");
    ASSERT_EQ(cb::engine_errc::success, store->set(item, cookie));
    store->getVBucket(vbid)->ht.findItem(item).storedValue->setFreqCounterValue(
            1);

    flushVBucketToDiskIfPersistent(vbid);
    getInitialMfuUpdaterTask()->run();

    // Regardless of eviction strategy, we should not see initial MFU <
    // Item::initialFreqCount.
    EXPECT_EQ(Item::initialFreqCount, store->getInitialMFU());
}

/**
 * The initial MFU is based on active MFU histograms only.
 */
TEST_P(InitialMFUTest, InitialMfuIgnoresReplicas) {
    setVBucketState(vbid, vbucket_state_active);

    auto item = make_item(vbid, makeStoredDocKey("1"), "");
    ASSERT_EQ(cb::engine_errc::success, store->set(item, cookie));
    store->getVBucket(vbid)->ht.findItem(item).storedValue->setFreqCounterValue(
            100);

    setVBucketState(vbid, vbucket_state_replica);
    flushVBucketToDiskIfPersistent(vbid);
    getInitialMfuUpdaterTask()->run();

    EXPECT_EQ(Item::initialFreqCount, store->getInitialMFU());
}

INSTANTIATE_TEST_SUITE_P(CouchstoreAllEvictionStrategies,
                         InitialMFUTest,
                         InitialMFUTest::configValues(),
                         InitialMFUTest::PrintToStringParamName);
