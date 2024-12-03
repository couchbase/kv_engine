/*
 *     Copyright 2024-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "task_signal_test.h"

#include "ep_task.h"
#include "executor/globaltask.h"
#include <folly/synchronization/Baton.h>

class SignalTest : public EpSignalTask {
public:
    SignalTest(EventuallyPersistentEngine& e,
               bool pauseExecution = false,
               bool runAgain = false)
        : EpSignalTask(e, TaskId::ItemPager, INT_MAX),
          pauseExecution(pauseExecution),
          runAgain(runAgain) {
    }

    std::string getDescription() const override {
        return "signal test task";
    }

    std::chrono::microseconds maxExpectedDuration() const override {
        // return any reasonable value, this is test only
        return std::chrono::microseconds(100);
    }

    bool runInner(bool manuallyNotified) override {
        if (pauseExecution) {
            // Notify that we are waiting
            isPaused.post();
            // Wait until we are told to continue
            shouldContinue.wait();
            shouldContinue.reset();
        }

        ++runs;
        return runAgain;
    }

    void waitUntilPaused() {
        if (pauseExecution) {
            // call will wait until this task enters pause state when running
            auto deadline = std::chrono::steady_clock::now() + 30s;
            if (!isPaused.try_wait_until(deadline)) {
                FAIL() << "Task did not pause within timeout";
            }
            isPaused.reset();
        }
    }

    void continueExecution() {
        if (pauseExecution) {
            shouldContinue.post();
        }
    }

    bool pauseExecution;
    folly::Baton<> shouldContinue;
    folly::Baton<> isPaused;

    std::atomic<bool> runAgain;
    std::atomic<size_t> runs = 0;
};

class WaiterTest : public cb::Waiter {
public:
    WaiterTest(std::function<void()> callback = []() {})
        : callback(std::move(callback)) {
    }
    void signal() override {
        signaled++;
        callback();
    }

    int getSignalCount() const {
        return signaled.load();
    }

private:
    std::atomic<int> signaled{0};
    std::function<void()> callback;
};

void TaskSignalTest::waitForTaskCompletion(SignalTest& task,
                                           std::chrono::seconds timeout) {
    using namespace std::chrono_literals;
    using std::chrono::steady_clock;
    auto deadline = steady_clock::now() + timeout;

    bool done;
    do {
        done = task.isdead();
        if (!done) {
            std::this_thread::sleep_for(5ms);
        }
    } while (!done && steady_clock::now() < deadline);

    if (!done) {
        FAIL() << "Task did not finish within timeout";
    }
}

std::shared_ptr<SignalTest> TaskSignalTest::makeAndScheduleTask(
        bool pauseExecution, bool runAgain) {
    auto task = std::make_shared<SignalTest>(*engine, pauseExecution, runAgain);
    ExecutorPool::get()->schedule(task);
    EXPECT_EQ(0, task->runs);
    EXPECT_EQ(TASK_SNOOZED, task->getState());
    EXPECT_EQ(std::chrono::steady_clock::time_point::max(),
              task->getWaketime());
    return task;
}

TEST_F(TaskSignalTest, Wakeup) {
    auto task = makeAndScheduleTask();

    task->wakeup();
    waitForTaskCompletion(*task);

    EXPECT_EQ(1, task->runs);
}

TEST_F(TaskSignalTest, WakeupAndGetNotified) {
    auto task = makeAndScheduleTask();

    auto waiter = std::make_shared<WaiterTest>();

    task->wakeupAndGetNotified(waiter);
    waitForTaskCompletion(*task);

    EXPECT_EQ(1, task->runs);
    EXPECT_EQ(1, waiter->getSignalCount());
}

TEST_F(TaskSignalTest, WakeupAndGetNotifiedConcurrent) {
    auto task = makeAndScheduleTask(true);

    auto waiter1 = std::make_shared<WaiterTest>();
    auto waiter2 = std::make_shared<WaiterTest>();

    task->wakeupAndGetNotified(waiter1);

    // Task is running
    task->waitUntilPaused();
    // During run, add second waiter
    task->wakeupAndGetNotified(waiter2);
    task->continueExecution();

    waitForTaskCompletion(*task);

    // Both waiters signaled once
    EXPECT_EQ(1, task->runs);
    EXPECT_EQ(1, waiter1->getSignalCount());
    EXPECT_EQ(1, waiter2->getSignalCount());
}

/**
 * This test demonstrates that if a waiter is added while the task is running,
 * but before completing, the waiter will be notified after the current/first
 * execution completes.
 * The task will be woken up again but the waiter will not be notified again.
 */
TEST_F(TaskSignalTest, WakeupAndGetNotifiedOnceWhileRunning) {
    auto task = makeAndScheduleTask(true, true);

    auto waiter = std::make_shared<WaiterTest>([&]() {
        // Task is scheduled to run again
        EXPECT_EQ(true, task->runAgain);
        // After next execution task will not run again
        task->runAgain = false;
    });

    task->wakeup();

    // Ensure the task starts running but should not complete
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    task->waitUntilPaused();
    // Add to the waiters list while task is on first execution
    task->wakeupAndGetNotified(waiter);
    // Complete first execution of task
    task->continueExecution();

    // Second execution of the task is running now
    task->waitUntilPaused();
    // Waiter should have been notified after first execution
    EXPECT_EQ(1, waiter->getSignalCount());

    // Complete the second execution
    task->continueExecution();
    waitForTaskCompletion(*task);

    // Task executed twice but waiter should be signaled only once
    EXPECT_EQ(2, task->runs);
    EXPECT_EQ(1, waiter->getSignalCount());
}
