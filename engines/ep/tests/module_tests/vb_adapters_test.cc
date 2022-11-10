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

#include "vb_adapters.h"
#include "vb_visitors.h"
#include "vbucket.h"

#include <folly/portability/GMock.h>
#include <folly/portability/GTest.h>

using namespace ::testing;

class VBAdaptorsTest : public SingleThreadedKVBucketTest {};

MATCHER_P(HasVbid, vbid, "") {
    return arg.getId() == vbid;
}

class MockVisitor : public InterruptableVBucketVisitor {
public:
    MockVisitor(ExecutionState stateToReturn) : stateToReturn(stateToReturn) {
    }

    ExecutionState shouldInterrupt() override {
        return stateToReturn;
    }

    MOCK_METHOD1(visitBucket, void(VBucket&));

    ExecutionState stateToReturn;
};

TEST_F(VBAdaptorsTest, VBCBAdaptorVisitsVbuckets) {
    setVBucketState(Vbid(0), vbucket_state_active);
    setVBucketState(Vbid(1), vbucket_state_active);
    setVBucketState(Vbid(2), vbucket_state_active);

    auto visitor = std::make_unique<StrictMock<MockVisitor>>(
            InterruptableVBucketVisitor::ExecutionState::Continue);

    EXPECT_CALL(*visitor, visitBucket(HasVbid(Vbid(0)))).Times(1);
    EXPECT_CALL(*visitor, visitBucket(HasVbid(Vbid(1)))).Times(1);
    EXPECT_CALL(*visitor, visitBucket(HasVbid(Vbid(2)))).Times(1);

    // Create an adapter for our dummy visitor. TaskId doesn't matter.
    auto task = std::make_shared<VBCBAdaptor>(store,
                                              TaskId::ItemPager,
                                              std::move(visitor),
                                              "",
                                              /*shutdown*/ false);

    // Should complete within 1 run().
    EXPECT_FALSE(task->run());
}

TEST_F(VBAdaptorsTest, PausingAdapterVisitsVbuckets) {
    setVBucketState(Vbid(0), vbucket_state_active);
    setVBucketState(Vbid(1), vbucket_state_active);
    setVBucketState(Vbid(2), vbucket_state_active);

    auto visitor = std::make_unique<StrictMock<MockVisitor>>(
            InterruptableVBucketVisitor::ExecutionState::Continue);

    EXPECT_CALL(*visitor, visitBucket(HasVbid(Vbid(0)))).Times(1);
    EXPECT_CALL(*visitor, visitBucket(HasVbid(Vbid(1)))).Times(1);
    EXPECT_CALL(*visitor, visitBucket(HasVbid(Vbid(2)))).Times(1);

    // Create an adapter for our dummy visitor. TaskId doesn't matter.
    auto task = std::make_shared<SingleSteppingVisitorAdapter>(
            store, TaskId::ItemPager, std::move(visitor), "", false);

    EXPECT_TRUE(task->run());
    EXPECT_TRUE(task->run());
    EXPECT_TRUE(task->run());
    // 4th run will just see that all vbuckets have been visited and return.
    EXPECT_FALSE(task->run());
}
