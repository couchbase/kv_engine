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

#include "kv_bucket.h"
#include "vb_visitors.h"
#include "vbucket.h"

#include <folly/portability/GMock.h>
#include <folly/portability/GTest.h>

using namespace ::testing;

class VBAdaptorsTest : public SingleThreadedKVBucketTest {};

MATCHER_P(HasVbid, vbid, "Check the provided VBucket has the given vbid") {
    return arg.getId() == vbid;
}

class MockInterruptVisitor : public InterruptableVBucketVisitor {
public:
    MOCK_METHOD0(shouldInterrupt, ExecutionState());
    MOCK_METHOD1(visitBucket, void(VBucket&));
};

TEST_F(VBAdaptorsTest, VBCBAdaptorDescription) {
    setVBucketState(Vbid(0), vbucket_state_active);
    setVBucketState(Vbid(1), vbucket_state_active);

    auto visitor = std::make_unique<StrictMock<MockInterruptVisitor>>();
    {
        InSequence s;
        EXPECT_CALL(*visitor, shouldInterrupt())
                .WillOnce(
                        Return(MockInterruptVisitor::ExecutionState::Continue));
        EXPECT_CALL(*visitor, visitBucket(HasVbid(Vbid(0))));
        EXPECT_CALL(*visitor, shouldInterrupt())
                .WillOnce(Return(MockInterruptVisitor::ExecutionState::Pause));

        EXPECT_CALL(*visitor, shouldInterrupt())
                .WillOnce(
                        Return(MockInterruptVisitor::ExecutionState::Continue));
        EXPECT_CALL(*visitor, visitBucket(HasVbid(Vbid(1))));
        // Done.
    }

    // Create an adapter for our dummy visitor. TaskId doesn't matter.
    auto task = std::make_shared<VBCBAdaptor>(store,
                                              TaskId::ItemPager,
                                              std::move(visitor),
                                              "Task",
                                              /*shutdown*/ false);

    EXPECT_EQ(task->getDescription(), "Task no vbucket assigned");

    EXPECT_TRUE(task->run());
    EXPECT_EQ(task->getDescription(), "Task on vb:0");

    EXPECT_FALSE(task->run());
    EXPECT_EQ(task->getDescription(), "Task on vb:1");
}
