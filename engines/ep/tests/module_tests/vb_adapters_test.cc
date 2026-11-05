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
#include "vb_adapters.h"
#include "vb_visitors.h"
#include "vbucket.h"

#include <folly/portability/GMock.h>
#include <folly/portability/GTest.h>

using namespace ::testing;

class VBAdaptorsTest : public SingleThreadedKVBucketTest {};

MATCHER_P(HasVbid, vbid, "Check the provided VBucket has the given vbid") {
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

TEST_F(VBAdaptorsTest, RevisitingVisitor) {
    class RevisitingVisitor : public InterruptableVBucketVisitor {
    public:
        RevisitingVisitor(std::vector<Vbid>& visited) : visited(visited) {
        }

        ExecutionState shouldInterrupt() override {
            return ExecutionState::Continue;
        }

        void visitBucket(VBucket& vb) override {
            visited.push_back(vb.getId());
        }

        NeedsRevisit needsToRevisitLast() override {
            switch (visited.size()) {
            case 1:
                return NeedsRevisit::YesLater;
            case 2:
                return NeedsRevisit::YesNow;
            default:
                return NeedsRevisit::No;
            }
        }

        std::vector<Vbid>& visited;
    };

    setVBucketState(Vbid(0), vbucket_state_active);
    setVBucketState(Vbid(1), vbucket_state_active);
    setVBucketState(Vbid(2), vbucket_state_active);

    std::vector<Vbid> visited;
    auto visitor = std::make_unique<RevisitingVisitor>(visited);

    // Create an adapter for our visitor. TaskId doesn't matter.
    auto task = std::make_shared<VBCBAdaptor>(store,
                                              TaskId::ItemPager,
                                              std::move(visitor),
                                              "",
                                              /*shutdown*/ false);

    // Should complete within 1 run().
    EXPECT_FALSE(task->run());

    EXPECT_THAT(visited,
                ElementsAre(Vbid(0), Vbid(1), Vbid(1), Vbid(2), Vbid(0)));
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
