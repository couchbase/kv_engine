/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2020-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "dcp/backfill-manager.h"
#include "evp_store_single_threaded_test.h"

#include <folly/portability/GMock.h>
#include <folly/portability/GTest.h>

using ::testing::InSequence;
using ::testing::Return;

class GMockDCPBackfill : public DCPBackfillIface {
public:
    MOCK_METHOD0(run, backfill_status_t());
    MOCK_METHOD0(cancel, void());
    MOCK_CONST_METHOD0(getVBucketId, Vbid());
    MOCK_CONST_METHOD0(isStreamDead, bool());
};

class GMockBackfillTracker : public BackfillTrackingIface {
public:
    MOCK_METHOD0(canAddBackfillToActiveQ, bool());
    MOCK_METHOD0(decrNumRunningBackfills, void());
};

class BackfillManagerTest : public SingleThreadedKVBucketTest {
protected:
    void SetUp() override {
        SingleThreadedKVBucketTest::SetUp();
        backfillMgr =
                std::make_shared<BackfillManager>(*engine->getKVBucket(),
                                                  backfillTracker,
                                                  engine->getConfiguration());
    }

    void TearDown() override {
        // Need to destroy engine & backfillManager objects before shutting
        // down ExecutorPool.
        backfillMgr.reset();
        SingleThreadedKVBucketTest::TearDown();
    }

    /**
     * For tests which are not interested in backfilLTracker, configure it
     * to accept an arbirary number of concurrent backfills.
     */
    void ignoreBackfillTracker() {
        EXPECT_CALL(backfillTracker, canAddBackfillToActiveQ())
                .WillRepeatedly(Return(true));
        EXPECT_CALL(backfillTracker, decrNumRunningBackfills())
                .WillRepeatedly(Return());
    }

    GMockBackfillTracker backfillTracker;
    std::shared_ptr<BackfillManager> backfillMgr;
};

/*
 * Check that active Backfills are scheduled in round-robin order
 * (0, 1, 2, 0, 1, 2, ...) until they complete.
 */
TEST_F(BackfillManagerTest, RoundRobin) {
    // Not interested in behaviour of backfillTracker for this test.
    ignoreBackfillTracker();

    auto backfill0 = std::make_unique<GMockDCPBackfill>();
    auto backfill1 = std::make_unique<GMockDCPBackfill>();
    auto backfill2 = std::make_unique<GMockDCPBackfill>();

    // Expectation - two backfills should be scheduled in turn while they
    // return backfill_success.
    {
        InSequence s;
        EXPECT_CALL(*backfill0, run())
                .WillOnce(Return(backfill_success))
                .RetiresOnSaturation();
        EXPECT_CALL(*backfill1, run())
                .WillOnce(Return(backfill_success))
                .RetiresOnSaturation();
        EXPECT_CALL(*backfill2, run())
                .WillOnce(Return(backfill_success))
                .RetiresOnSaturation();
        EXPECT_CALL(*backfill0, run())
                .WillOnce(Return(backfill_finished))
                .RetiresOnSaturation();
        EXPECT_CALL(*backfill1, run())
                .WillOnce(Return(backfill_finished))
                .RetiresOnSaturation();
        EXPECT_CALL(*backfill2, run())
                .WillOnce(Return(backfill_finished))
                .RetiresOnSaturation();
    }

    // Test: schedule both backfills, then instruct the backfill manager to
    // backfill 6 times.
    ASSERT_EQ(BackfillManager::ScheduleResult::Active,
              backfillMgr->schedule(std::move(backfill0)));
    ASSERT_EQ(BackfillManager::ScheduleResult::Active,
              backfillMgr->schedule(std::move(backfill1)));
    ASSERT_EQ(BackfillManager::ScheduleResult::Active,
              backfillMgr->schedule(std::move(backfill2)));
    backfillMgr->backfill();
    backfillMgr->backfill();
    backfillMgr->backfill();
    backfillMgr->backfill();
    backfillMgr->backfill();
    backfillMgr->backfill();
}

/*
 * MB-37680: Check that active backfills are scheduled in sequential order when
 * backfillOrder is set to Sequential.
 */
TEST_F(BackfillManagerTest, Sequential) {
    // Not interested in behaviour of backfillTracker for this test.
    ignoreBackfillTracker();

    auto backfill0 = std::make_unique<GMockDCPBackfill>();
    auto backfill1 = std::make_unique<GMockDCPBackfill>();
    auto backfill2 = std::make_unique<GMockDCPBackfill>();

    // Expectation - each Backfill should be run once (to initialise and
    // generate the snapshot_marker, then the first backfill should be run to
    // completion before the next one starts.
    //
    {
        InSequence s;

        // each backfill called once for snapshot_marker
        EXPECT_CALL(*backfill0, run())
                .WillOnce(Return(backfill_success))
                .RetiresOnSaturation();
        EXPECT_CALL(*backfill1, run())
                .WillOnce(Return(backfill_success))
                .RetiresOnSaturation();
        EXPECT_CALL(*backfill2, run())
                .WillOnce(Return(backfill_success))
                .RetiresOnSaturation();

        // then each one sequential until finished.
        EXPECT_CALL(*backfill0, run())
                .WillOnce(Return(backfill_success))
                .WillOnce(Return(backfill_finished))
                .RetiresOnSaturation();
        EXPECT_CALL(*backfill1, run())
                .WillOnce(Return(backfill_success))
                .WillOnce(Return(backfill_finished))
                .RetiresOnSaturation();
        EXPECT_CALL(*backfill2, run())
                .WillOnce(Return(backfill_success))
                .WillOnce(Return(backfill_finished))
                .RetiresOnSaturation();
    }

    // Test: schedule all backfills, then instruct the backfill manager to
    // backfill 9 times.
    backfillMgr->setBackfillOrder(BackfillManager::ScheduleOrder::Sequential);
    ASSERT_EQ(BackfillManager::ScheduleResult::Active,
              backfillMgr->schedule(std::move(backfill0)));
    ASSERT_EQ(BackfillManager::ScheduleResult::Active,
              backfillMgr->schedule(std::move(backfill1)));
    ASSERT_EQ(BackfillManager::ScheduleResult::Active,
              backfillMgr->schedule(std::move(backfill2)));
    for (int i = 0; i < 9; i++) {
        backfillMgr->backfill();
    }
}

/**
 * Check that if BackfillTracker is full then BackfillManager correctly
 * puts scheduled Backfills into PendingQ, until space becomes available.
 */
TEST_F(BackfillManagerTest, BackfillTrackerFull) {
    auto backfill0 = std::make_unique<GMockDCPBackfill>();
    auto backfill1 = std::make_unique<GMockDCPBackfill>();

    // Configure expectations.
    // (Note: must configure all GMockDCPBackfill expectations before
    //  schedule() is called, as that takes ownership of the object).
    {
        InSequence s;
        // Calls to schedule() for the two backfills. BackfillManager should
        // check if it is allowed to add backfill to activeQ. backfill0 should
        // be allowed, backfill1 should not (and hence be put in pendingQ).
        EXPECT_CALL(backfillTracker, canAddBackfillToActiveQ())
                .WillOnce(Return(true))
                .WillOnce(Return(false))
                .RetiresOnSaturation();

        // Next, when backfill() is called, it should again check if there are
        // items in pending queue (which there is 1), and if they they can be
        // moved to activeQ. Given backfill0 has not finished (and made space),
        // return false here.
        EXPECT_CALL(backfillTracker, canAddBackfillToActiveQ())
                .WillOnce(Return(false))
                .RetiresOnSaturation();
        // backfill0 should be run now, we return success.
        EXPECT_CALL(*backfill0, run())
                .WillOnce(Return(backfill_success))
                .RetiresOnSaturation();

        // Second call to backfill() will again attempt to add the pending item
        // to activeQ. Again tracker denies it.
        EXPECT_CALL(backfillTracker, canAddBackfillToActiveQ())
                .WillOnce(Return(false))
                .RetiresOnSaturation();
        // backfill0 should run a second time, we return finished.
        EXPECT_CALL(*backfill0, run())
                .WillOnce(Return(backfill_finished))
                .RetiresOnSaturation();
        // Upon backfill_finished, backfill manager should decrement the number
        // of actve/snoozing backfills.
        EXPECT_CALL(backfillTracker, decrNumRunningBackfills())
                .WillOnce(Return())
                .RetiresOnSaturation();

        // Third call to backfill() - the pending backfill1 should now be
        // allowed to be added to activeQ.
        EXPECT_CALL(backfillTracker, canAddBackfillToActiveQ())
                .WillOnce(Return(true))
                .RetiresOnSaturation();
        // backfill1 should be run now.
        EXPECT_CALL(*backfill1, run())
                .WillOnce(Return(backfill_success))
                .RetiresOnSaturation();

        // Fourth call to backfill() - no items in pendingQ so no call to
        // backfilLTracker this fine; backfill1 should finish and decrment
        // active/snoozing backfills
        EXPECT_CALL(*backfill1, run())
                .WillOnce(Return(backfill_finished))
                .RetiresOnSaturation();
        EXPECT_CALL(backfillTracker, decrNumRunningBackfills())
                .WillOnce(Return())
                .RetiresOnSaturation();
    }

    // Test - schedule the two backfills - first should be added to active Q,
    // second to pending Q.
    ASSERT_EQ(BackfillManager::ScheduleResult::Active,
              backfillMgr->schedule(std::move(backfill0)));
    ASSERT_EQ(BackfillManager::ScheduleResult::Pending,
              backfillMgr->schedule(std::move(backfill1)));

    // Drive manager forward - see above expectations.
    backfillMgr->backfill();
    backfillMgr->backfill();
    backfillMgr->backfill();
    backfillMgr->backfill();
}

/**
 * Check that if BackfillManager is destroyed with Backfills in the
 * initializingQ, then it correctly notifies the backfill tracker when the
 * BackfillManager is destroyed.
 */
TEST_F(BackfillManagerTest, InitializingQNotifiesTrackerOnDtor) {
    auto backfill = std::make_unique<GMockDCPBackfill>();

    // Configure expectations.
    // (Note: must configure all GMockDCPBackfill expectations before
    //  schedule() is called, as that takes ownership of the object).
    {
        InSequence s;
        // Call to schedule() for the backfills. BackfillManager should
        // check if it is allowed to add backfill to initializingQ. Backfill
        // should be allowed, which increments tracker's count.
        EXPECT_CALL(backfillTracker, canAddBackfillToActiveQ())
                .WillOnce(Return(true))
                .RetiresOnSaturation();

        // When BackfillManager is destroyed, the incomplete backfill in
        // initializingQ should be decremented in the tracker.
        EXPECT_CALL(backfillTracker, decrNumRunningBackfills())
                .WillOnce(Return())
                .RetiresOnSaturation();
    }

    // Setup: schedule a single backfill.
    ASSERT_EQ(BackfillManager::ScheduleResult::Active,
              backfillMgr->schedule(std::move(backfill)));

    // Test: Destroy the backfill manager while backfill still in initializingQ.
    backfillMgr.reset();
}

/**
 * Check that if BackfillManager is destroyed with Backfills in the activeQ,
 * then it correctly notifies the backfill tracker when the BackfillManager is
 * destroyed.
 */
TEST_F(BackfillManagerTest, ActiveQNotifiesTrackerOnDtor) {
    auto backfill = std::make_unique<GMockDCPBackfill>();

    // Configure expectations.
    // (Note: must configure all GMockDCPBackfill expectations before
    //  schedule() is called, as that takes ownership of the object).
    {
        InSequence s;
        // Call to schedule() for the backfills. BackfillManager should
        // check if it is allowed to add backfill to initializingQ. Backfill
        // should be allowed, which increments tracker's count.
        EXPECT_CALL(backfillTracker, canAddBackfillToActiveQ())
                .WillOnce(Return(true))
                .RetiresOnSaturation();

        // Call to backfill(). Want to move backfill into the activeQ.
        EXPECT_CALL(*backfill, run())
                .WillOnce(Return(backfill_success))
                .RetiresOnSaturation();

        // When BackfillManager is destroyed, the incomplete backfill in
        // activeQ should be decremented in the tracker.
        EXPECT_CALL(backfillTracker, decrNumRunningBackfills())
                .WillOnce(Return())
                .RetiresOnSaturation();
    }

    // Setup: schedule a single backfill and run it once so it can be moved
    // to activeQ.
    ASSERT_EQ(BackfillManager::ScheduleResult::Active,
              backfillMgr->schedule(std::move(backfill)));
    backfillMgr->backfill();

    // Test: Destroy the backfill manager while backfill still in activeQ.
    backfillMgr.reset();
}

/**
 * Check that if BackfillManager is destroyed with Backfills in the snoozingQ,
 * then it correctly notifies the backfill tracker when the Backfill is
 * destroyed.
 */
TEST_F(BackfillManagerTest, SnoozingQNotifiesTrackerOnDtor) {
    auto backfill = std::make_unique<GMockDCPBackfill>();

    // Configure expectations.
    // (Note: must configure all GMockDCPBackfill expectations before
    //  schedule() is called, as that takes ownership of the object).
    {
        InSequence s;
        // Call to schedule() for the backfills. BackfillManager should
        // check if it is allowed to add backfill to activeQ. Backfill should
        // be allowed, which increments tracker's count.
        EXPECT_CALL(backfillTracker, canAddBackfillToActiveQ())
                .WillOnce(Return(true))
                .RetiresOnSaturation();

        // Call to backfill(). Want to move backfill into the snoozinqQ.
        EXPECT_CALL(*backfill, run())
                .WillOnce(Return(backfill_snooze))
                .RetiresOnSaturation();

        // When BackfillManager is destroyed, the incomplete backfill in
        // snoozingQ should be decremented in the tracker.
        EXPECT_CALL(backfillTracker, decrNumRunningBackfills())
                .WillOnce(Return())
                .RetiresOnSaturation();
    }

    // Setup: schedule a single backfill and run it one so it can be moved
    // to snoozingQ.
    ASSERT_EQ(BackfillManager::ScheduleResult::Active,
              backfillMgr->schedule(std::move(backfill)));
    backfillMgr->backfill();

    // Test: Destroy the backfill manager while backfill still in snoozingQ.
    backfillMgr.reset();
}
