/*
 *     Copyright 2023-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include <folly/portability/GMock.h>
#include <folly/portability/GTest.h>

#include <daemon/yielding_limited_concurrency_task.h>
#include <daemon/yielding_task.h>
#include <executor/executorpool.h>
#include <executor/fake_executorpool.h>

#include <chrono>
#include <optional>
#include <utility>

/**
 * Base fixture for tests involving daemon-related task utilities.
 */
class TasksTest : public ::testing::Test {
public:
    void SetUp() override {
        ExecutorPool::create(ExecutorPool::Backend::Fake);
        ExecutorPool::get()->registerTaskable(NoBucketTaskable::instance());
    }
    void TearDown() override {
        auto& executor =
                dynamic_cast<SingleThreadedExecutorPool&>(*ExecutorPool::get());
        executor.cancelAndClearAll();
        executor.unregisterTaskable(NoBucketTaskable::instance(), true);
        ExecutorPool::shutdown();
    }
};

using MockTaskFunc = testing::StrictMock<
        testing::MockFunction<std::optional<std::chrono::duration<double>>()>>;

/**
 * Test that a task is called again if it returns a snooze time,
 * and is not called again when returning a nullopt to indicate it is "done"
 */
static void testTaskCalledAgain(ExTask task, MockTaskFunc& mockTaskFunc) {
    using namespace testing;

    ExecutorPool::get()->schedule(task);

    InSequence s;

    using namespace std::chrono_literals;
    EXPECT_CALL(mockTaskFunc, Call()).Times(1).WillOnce(Return(0s));

    EXPECT_CALL(mockTaskFunc, Call()).Times(1).WillOnce(Return(std::nullopt));

    auto& executor =
            dynamic_cast<SingleThreadedExecutorPool&>(*ExecutorPool::get());
    executor.runNextTask(NONIO_TASK_IDX, "foobar");
    executor.runNextTask(NONIO_TASK_IDX, "foobar");
    // doesn't need to run again
    EXPECT_THROW(executor.runNextTask(NONIO_TASK_IDX, "foobar"),
                 std::logic_error);
}

TEST_F(TasksTest, YieldingTaskCalledAgain) {
    MockTaskFunc func;
    testTaskCalledAgain(
            std::make_shared<YieldingTask>(TaskId::Core_DeleteBucketTask,
                                           "foobar",
                                           func.AsStdFunction(),
                                           std::chrono::seconds(30)),
            func);
}

TEST_F(TasksTest, YieldingLimitedConcurrencyTaskCalledAgain) {
    MockTaskFunc func;
    cb::AwaitableSemaphore semaphore{4};
    testTaskCalledAgain(std::make_shared<YieldingLimitedConcurrencyTask>(
                                TaskId::Core_DeleteBucketTask,
                                "foobar",
                                func.AsStdFunction(),
                                semaphore,
                                std::chrono::seconds(30)),
                        func);
}

/**
 * Test that a task is correctly snooze()'ed
 */
static void testTaskSnoozes(ExTask task, MockTaskFunc& mockTaskFunc) {
    using namespace testing;

    ExecutorPool::get()->schedule(task);

    using namespace std::chrono;
    using namespace std::chrono_literals;
    // return a long snooze time
    EXPECT_CALL(mockTaskFunc, Call()).Times(1).WillOnce(Return(10min));

    const auto beforeTime = std::chrono::steady_clock::now();
    auto& executor =
            dynamic_cast<SingleThreadedExecutorPool&>(*ExecutorPool::get());
    executor.runNextTask(NONIO_TASK_IDX, "foobar");
    // doesn't need to run again yet, should be snoozed for 10 mins
    EXPECT_THROW(executor.runNextTask(NONIO_TASK_IDX, "foobar"),
                 std::logic_error);

    EXPECT_EQ(TASK_SNOOZED, task->getState());
    EXPECT_GE(task->getWaketime(), beforeTime + 10min);
}

TEST_F(TasksTest, YieldingTaskSnoozes) {
    MockTaskFunc func;
    testTaskSnoozes(
            std::make_shared<YieldingTask>(TaskId::Core_DeleteBucketTask,
                                           "foobar",
                                           func.AsStdFunction(),
                                           std::chrono::seconds(30)),
            func);
}

TEST_F(TasksTest, YieldingLimitedConcurrencyTaskSnoozes) {
    MockTaskFunc func;
    cb::AwaitableSemaphore semaphore{4};
    testTaskSnoozes(std::make_shared<YieldingLimitedConcurrencyTask>(
                            TaskId::Core_DeleteBucketTask,
                            "foobar",
                            func.AsStdFunction(),
                            semaphore,
                            std::chrono::seconds(30)),
                    func);
}
