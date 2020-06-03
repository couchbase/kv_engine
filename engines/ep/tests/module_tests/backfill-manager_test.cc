/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2020 Couchbase, Inc
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

class BackfillManagerTest : public SingleThreadedKVBucketTest {
protected:
    void SetUp() override {
        SingleThreadedKVBucketTest::SetUp();
        backfillMgr = std::make_shared<BackfillManager>(*engine);
    }

    void TearDown() override {
        // Need to destroy engine & backfillManager objects before shutting
        // down ExecutorPool.
        backfillMgr.reset();
        SingleThreadedKVBucketTest::TearDown();
    }

    std::shared_ptr<BackfillManager> backfillMgr;
};

/*
 * Check that active Backfills are scheduled in round-robin order
 * (0, 1, 2, 0, 1, 2, ...) until they complete.
 */
TEST_F(BackfillManagerTest, RoundRobin) {
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
    auto backfill0 = std::make_unique<GMockDCPBackfill>();
    auto backfill1 = std::make_unique<GMockDCPBackfill>();
    auto backfill2 = std::make_unique<GMockDCPBackfill>();

    // Expectation - the first backfill should be run to completion before the
    // next one starts.
    //
    {
        InSequence s;
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
    // backfill 6 times.
    backfillMgr->setBackfillOrder(BackfillManager::ScheduleOrder::Sequential);
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
