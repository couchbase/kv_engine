/*
 *     Copyright 2022-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "../mock/mock_synchronous_ep_engine.h"
#include "ep_bucket.h"
#include "ep_engine.h"
#include "evp_store_single_threaded_test.h"
#include "tests/mock/mock_ep_bucket.h"
#include <folly/CancellationToken.h>
#include <platform/awaitable_semaphore.h>

/**
 * Test fixture for Pause / Resume related KVBucket tests.
 */
class PauseResumeTest : public STParameterizedBucketTest {};

/// Basic test that a Bucket can be paused and then resumed.
TEST_P(PauseResumeTest, Basic) {
    EXPECT_EQ(cb::engine_errc::success, store->prepareForPause({}));
    store->prepareForResume();
}

/**
 * Test fixture for Pause / Resume related EPBucket tests.
 */
class PauseResumeEPTest : public STParameterizedBucketTest {
protected:
    void SetUp() override {
        STParameterizedBucketTest::SetUp();
        ASSERT_GE(store->getVBMapSize(), 2)
                << "Require at least 2 vBuckets in map to test cancellation "
                   "between vBuckets";
        ASSERT_GE(2, engine->getConfiguration().getMaxNumShards())
                << "Require at least 2 shards to test cancelleation between "
                   "shards";

        auto& mockEP = dynamic_cast<MockEPBucket&>(*store);
        engine->getWorkLoadPolicy().setWorkLoadPattern(WRITE_HEAVY);
        engine->getConfiguration().setCompactionMaxConcurrentRatio(1.0);
        mockEP.public_updateCompactionConcurrency();
        ASSERT_GT(mockEP.public_getCompactionSemaphore().getCapacity(), 1)
                << "Require more compactionSemaphore capacity greater than 1 "
                   "to be able to verify that unpause / cancel has returned it "
                   "to initial value (it is reduced to 1 during pause)";
    }

    // For all tests in this suite, at the end of the test the Bucket should
    // be the same initial state (i.e. unpaused) with no additional
    // resources held. Check that in a common set of postconditions here.
    void TearDown() override {
        // Verify that all vb_mutexes are unlocked.
        for (Vbid::id_type id = 0; id < store->getVBMapSize(); id++) {
            Vbid vb{id};
            auto lockedVB = store->getLockedVBucket(vb, std::try_to_lock);
            EXPECT_TRUE(lockedVB.owns_lock())
                    << "Failed to lock " << vb
                    << " - all vbuckets should be unlocked at end of test";
        }

        // Verify that compactionSemaphore has the correct capacity and is
        // not held.
        auto& mockEP = dynamic_cast<MockEPBucket&>(*store);
        EXPECT_GT(mockEP.public_getCompactionSemaphore().getCapacity(), 1);

        // Confirm empirically that bucket is in an unpaused state by testing
        // we can indeed pause it.
        EXPECT_EQ(cb::engine_errc::success, store->prepareForPause({}));

        STParameterizedBucketTest::TearDown();
    }

    void testCancellableDuringVBMutexLock(bool lockVB);
};

void PauseResumeEPTest::testCancellableDuringVBMutexLock(bool lockVB) {
    folly::CancellationSource cs;
    auto& epBucket = dynamic_cast<EPBucket&>(*store);
    int vbCount = 0;
    epBucket.prepareForPauseTestingHook = [&](auto phase) {
        if (phase == "Lock vb_mutexes" && ++vbCount == 2) {
            cs.requestCancellation();
        }
    };
    {
        auto lockedVB = epBucket.getLockedVBucket(Vbid{1});
        if (!lockVB) {
            lockedVB.getLock().unlock();
        }
        EXPECT_EQ(cb::engine_errc::cancelled,
                  store->prepareForPause(cs.getToken()));
    }
    epBucket.prepareForPauseTestingHook.reset();
}

/// Test that pausing can be Cancelled during locking of vBucket mutexes, and
/// that a subsequent pause() request (without Cancellation) succeeds.
TEST_P(PauseResumeEPTest, CancellableDuringVBMutexLock) {
    testCancellableDuringVBMutexLock(false);
}

/// Test that pausing can be Cancelled during locking of vBucket mutexes, when
/// one of the vb_mutexes is locked and hence the initial lock attempt must
/// be retried.
TEST_P(PauseResumeEPTest, CancellableDuringVBMutexLockWhileLocked) {
    testCancellableDuringVBMutexLock(true);
}

/// Test that pausing can be Cancelled during pausing of the underlying
/// KVStores.
TEST_P(PauseResumeEPTest, CancellableDuringKVStorePause) {
    // Trigger cancellation when inside prepareForPause for the second vb_mutex
    // lock.
    folly::CancellationSource cs;
    auto& epBucket = dynamic_cast<EPBucket&>(*store);
    int shardCount = 0;
    epBucket.prepareForPauseTestingHook = [&](auto phase) {
        if (phase == "Pause KVStore" && ++shardCount == 2) {
            cs.requestCancellation();
        }
    };
    EXPECT_EQ(cb::engine_errc::cancelled,
              store->prepareForPause(cs.getToken()));

    epBucket.prepareForPauseTestingHook.reset();
}

INSTANTIATE_TEST_SUITE_P(AllBackends,
                         PauseResumeTest,
                         STParameterizedBucketTest::allConfigValues(),
                         STParameterizedBucketTest::PrintToStringParamName);

INSTANTIATE_TEST_SUITE_P(Persistent,
                         PauseResumeEPTest,
                         STParameterizedBucketTest::persistentConfigValues(),
                         STParameterizedBucketTest::PrintToStringParamName);
