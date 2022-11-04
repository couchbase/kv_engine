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
#include "cross_bucket_visitor_adapter.h"
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

TEST_F(VBAdaptorsTest, PausingAdapterVisitsVbuckets) {
    setVBucketState(Vbid(0), vbucket_state_active);
    setVBucketState(Vbid(1), vbucket_state_active);
    setVBucketState(Vbid(2), vbucket_state_active);

    auto visitor = std::make_unique<StrictMock<MockVisitor>>(
            InterruptableVBucketVisitor::ExecutionState::Continue);

    EXPECT_CALL(*visitor, visitBucket(HasVbid(Vbid(0)))).Times(1);
    EXPECT_CALL(*visitor, visitBucket(HasVbid(Vbid(1)))).Times(1);
    EXPECT_CALL(*visitor, visitBucket(HasVbid(Vbid(2)))).Times(1);

    StrictMock<MockFunction<void(const CallbackAdapter&, bool)>> mockCallback;
    // We run the task 4 times so we should get 4 callbacks.
    {
        InSequence seq;
        EXPECT_CALL(mockCallback, Call(_, true));
        EXPECT_CALL(mockCallback, Call(_, true));
        EXPECT_CALL(mockCallback, Call(_, true));
        EXPECT_CALL(mockCallback, Call(_, false));
    }

    // Create an adapter for our dummy visitor. TaskId doesn't matter.
    auto task = std::make_shared<SingleSteppingVisitorAdapter>(
            store,
            TaskId::ItemPager,
            std::move(visitor),
            "",
            false,
            mockCallback.AsStdFunction());

    EXPECT_TRUE(task->run());
    EXPECT_TRUE(task->run());
    EXPECT_TRUE(task->run());
    // 4th run will just see that all vbuckets have been visited and return.
    EXPECT_FALSE(task->run());
}

class TestVisitor : public InterruptableVBucketVisitor {
public:
    TestVisitor(std::optional<Vbid>& lastVbid) : lastVbid(lastVbid) {
    }

    ExecutionState shouldInterrupt() override {
        return ExecutionState::Continue;
    }

    void visitBucket(VBucket& vb) override {
        lastVbid = vb.getId();
    }

    std::optional<Vbid>& lastVbid;
};

TEST_F(VBAdaptorsTest, CrossBucketVisitorsWorksForSingleBucket) {
    setVBucketState(Vbid(0), vbucket_state_active);
    setVBucketState(Vbid(1), vbucket_state_active);
    setVBucketState(Vbid(2), vbucket_state_active);

    auto* bucketApi = engine->getServerApi()->bucket;
    auto handle = *bucketApi->tryAssociateBucket(engine.get());

    // Track the last Vbid the visitor has seen.
    std::optional<Vbid> lastVbid;
    auto visitor = std::make_unique<TestVisitor>(lastVbid);

    CrossBucketVisitorAdapter::VisitorMap visitors;
    visitors.emplace_back(std::move(handle), std::move(visitor));

    auto crossBucketVisitor = std::make_shared<CrossBucketVisitorAdapter>(
            *bucketApi,
            std::move(visitors),
            // ScheduleOrder doesn't matter for a single bucket
            CrossBucketVisitorAdapter::ScheduleOrder::RoundRobin,
            TaskId::ItemPager,
            "test",
            std::chrono::microseconds(0));
    crossBucketVisitor->scheduleNow();

    EXPECT_FALSE(crossBucketVisitor->hasCompleted());
    // Expect to visit one Vbucket per task wakeup.
    EXPECT_EQ(std::nullopt, lastVbid);
    task_executor->runNextTask(NONIO_TASK_IDX,
                               "test (SynchronousEPEngine:default)");
    EXPECT_EQ(Vbid(0), lastVbid);
    task_executor->runNextTask(NONIO_TASK_IDX,
                               "test (SynchronousEPEngine:default)");
    EXPECT_EQ(Vbid(1), lastVbid);
    task_executor->runNextTask(NONIO_TASK_IDX,
                               "test (SynchronousEPEngine:default)");
    EXPECT_EQ(Vbid(2), lastVbid);
    // Final task run will not progress the visitor, as all vBuckets have been
    // visited.
    task_executor->runNextTask(NONIO_TASK_IDX,
                               "test (SynchronousEPEngine:default)");
    EXPECT_EQ(Vbid(2), lastVbid);
    // The task should have ran to completion.
    EXPECT_THROW(task_executor->runNextTask(
                         NONIO_TASK_IDX, "test (SynchronousEPEngine:default)"),
                 std::logic_error);
    EXPECT_TRUE(crossBucketVisitor->hasCompleted());
}

TEST_F(VBAdaptorsTest, CrossBucketVisitorsWorksForTwoBuckets) {
    setVBucketState(Vbid(0), vbucket_state_active);
    setVBucketState(Vbid(1), vbucket_state_active);
    setVBucketState(Vbid(2), vbucket_state_active);

    auto engine2 = SynchronousEPEngine::build(
            "dbname=CrossBucketVisitorsWorksForTwoBuckets;couch_bucket="
            "engine2");
    ASSERT_EQ(cb::engine_errc::success,
              engine2->getKVBucket()->setVBucketState(Vbid(0),
                                                      vbucket_state_active));
    ASSERT_EQ(cb::engine_errc::success,
              engine2->getKVBucket()->setVBucketState(Vbid(1),
                                                      vbucket_state_active));

    auto* bucketApi = engine->getServerApi()->bucket;
    auto bucket1Handle = *bucketApi->tryAssociateBucket(engine.get());
    auto bucket2Handle = *bucketApi->tryAssociateBucket(engine2.get());

    CrossBucketVisitorAdapter::VisitorMap visitors;
    std::optional<Vbid> lastVbidBucket1;
    visitors.emplace_back(std::move(bucket1Handle),
                          std::make_unique<TestVisitor>(lastVbidBucket1));
    std::optional<Vbid> lastVbidBucket2;
    visitors.emplace_back(std::move(bucket2Handle),
                          std::make_unique<TestVisitor>(lastVbidBucket2));

    auto crossBucketVisitor = std::make_shared<CrossBucketVisitorAdapter>(
            *bucketApi,
            std::move(visitors),
            CrossBucketVisitorAdapter::ScheduleOrder::RoundRobin,
            TaskId::ItemPager,
            "test",
            std::chrono::microseconds(0),
            nullptr,
            /* randomShuffle */ false);
    crossBucketVisitor->scheduleNow();

    // Expect to visit one Vbucket per task wakeup.
    EXPECT_EQ(std::nullopt, lastVbidBucket1);
    EXPECT_EQ(std::nullopt, lastVbidBucket2);
    {
        task_executor->runNextTask(NONIO_TASK_IDX,
                                   "test (SynchronousEPEngine:default)");
        EXPECT_EQ(Vbid(0), lastVbidBucket1);
        EXPECT_EQ(std::nullopt, lastVbidBucket2);
    }
    {
        task_executor->runNextTask(NONIO_TASK_IDX,
                                   "test (SynchronousEPEngine:engine2)");
        EXPECT_EQ(Vbid(0), lastVbidBucket1);
        EXPECT_EQ(Vbid(0), lastVbidBucket2);
    }
    {
        task_executor->runNextTask(NONIO_TASK_IDX,
                                   "test (SynchronousEPEngine:default)");
        EXPECT_EQ(Vbid(1), lastVbidBucket1);
        EXPECT_EQ(Vbid(0), lastVbidBucket2);
    }
    {
        task_executor->runNextTask(NONIO_TASK_IDX,
                                   "test (SynchronousEPEngine:engine2)");
        EXPECT_EQ(Vbid(1), lastVbidBucket1);
        EXPECT_EQ(Vbid(1), lastVbidBucket2);
    }
    {
        task_executor->runNextTask(NONIO_TASK_IDX,
                                   "test (SynchronousEPEngine:default)");
        EXPECT_EQ(Vbid(2), lastVbidBucket1);
        EXPECT_EQ(Vbid(1), lastVbidBucket2);
    }
    {
        // Final task run will not progress the visitors
        task_executor->runNextTask(NONIO_TASK_IDX,
                                   "test (SynchronousEPEngine:engine2)");
        task_executor->runNextTask(NONIO_TASK_IDX,
                                   "test (SynchronousEPEngine:default)");
        EXPECT_EQ(Vbid(2), lastVbidBucket1);
        EXPECT_EQ(Vbid(1), lastVbidBucket2);
    }
    // The tasks should have ran to completion.
    EXPECT_THROW(task_executor->runNextTask(
                         NONIO_TASK_IDX, "test (SynchronousEPEngine:default)"),
                 std::logic_error);
    EXPECT_THROW(task_executor->runNextTask(
                         NONIO_TASK_IDX, "test (SynchronousEPEngine:engine2)"),
                 std::logic_error);
}

/**
 * Tasks managed by the cross-bucket adapter might get woken up when their
 * engine is shutting down. Test that we can detect this and ignore the tasks.
 */
TEST_F(VBAdaptorsTest, CrossBucketVisitorIgnoresUnexpectedWakeups) {
    setVBucketState(Vbid(0), vbucket_state_active);
    setVBucketState(Vbid(1), vbucket_state_active);

    auto* bucketApi = engine->getServerApi()->bucket;

    // Track the last Vbid the visitor has seen.
    std::optional<Vbid> lastVbid1;
    std::optional<Vbid> lastVbid2;

    CrossBucketVisitorAdapter::VisitorMap visitors;
    visitors.emplace_back(*bucketApi->tryAssociateBucket(engine.get()),
                          std::make_unique<TestVisitor>(lastVbid1));
    visitors.emplace_back(*bucketApi->tryAssociateBucket(engine.get()),
                          std::make_unique<TestVisitor>(lastVbid2));

    auto crossBucketVisitor = std::make_shared<CrossBucketVisitorAdapter>(
            *bucketApi,
            std::move(visitors),
            CrossBucketVisitorAdapter::ScheduleOrder::RoundRobin,
            TaskId::ItemPager,
            "test",
            std::chrono::microseconds(0));

    bool wasHookExecuted = false;
    crossBucketVisitor->scheduleNextHook = [&wasHookExecuted](
                                                   std::deque<ExTask>& queue,
                                                   GlobalTask* expected) {
        // Make the unexpected task run instead.
        auto unexpected =
                queue.front().get() == expected ? queue.back() : queue.front();
        // The task will signal its completion from within
        // GlobalTask::run() and callback into the CrossBucket adapter.
        // We should detect this and remove the task from the queue.
        EXPECT_THROW(unexpected->execute(""), std::logic_error);
        unexpected->getEngine()->getEpStats().isShutdown = true;
        EXPECT_NO_THROW(unexpected->execute(""));
        // The unexpected task is removed from the queue.
        EXPECT_EQ(1, queue.size());
        EXPECT_EQ(expected, queue.front().get());
        wasHookExecuted = true;
    };
    crossBucketVisitor->scheduleNow();
    ASSERT_TRUE(wasHookExecuted);
}
