/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2022-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "task_concurrency_test.h"

#include "limited_concurrency_task.h"

#include "../mock/mock_synchronous_ep_engine.h"
#include <platform/awaitable_semaphore.h>
#include <platform/semaphore_guard.h>

#include <utility>

#include "thread_gate.h"

class ConcurrencyTestTask : public LimitedConcurrencyTask {
public:
    using Callback = TaskConcurrencyTest::Callback;
    ConcurrencyTestTask(EventuallyPersistentEngine& e,
                        cb::AwaitableSemaphore& semaphore,
                        Callback callback)
        : LimitedConcurrencyTask(e, TaskId::ItemPager, semaphore, false),
          callback(std::move(callback)) {
    }
    bool runInner() override {
        runs++;
        auto guard = acquireOrWait();
        if (guard) {
            runsAcquiredToken++;
        }
        callback(guard);

        // If we didn't get a token we want to run again.
        // We _should_ already be snoozed forever.
        return !guard;
    };

    std::string getDescription() const override {
        return "test task";
    }

    std::chrono::microseconds maxExpectedDuration() const override {
        // return any reasonable value, this is test only
        return std::chrono::microseconds(100);
    }

    Callback callback;
    int runs = 0;
    int runsAcquiredToken = 0;
};

std::vector<std::shared_ptr<ConcurrencyTestTask>>
TaskConcurrencyTest::makeTasks(cb::AwaitableSemaphore& sem,
                               size_t taskCount,
                               ConcurrencyTestTask::Callback callback) {
    std::vector<std::shared_ptr<ConcurrencyTestTask>> tasks;
    for (size_t i = 0; i < taskCount; i++) {
        tasks.push_back(
                std::make_shared<ConcurrencyTestTask>(*engine, sem, callback));
    }
    return tasks;
}

void TaskConcurrencyTest::scheduleAll(
        std::vector<std::shared_ptr<ConcurrencyTestTask>>& tasks) {
    for (auto& task : tasks) {
        ExecutorPool::get()->schedule(task);
    }
}

void TaskConcurrencyTest::waitForTasks(
        std::vector<std::shared_ptr<ConcurrencyTestTask>>& tasks) {
    using namespace std::chrono_literals;
    using std::chrono::steady_clock;
    auto deadline = steady_clock::now() + 5s;

    bool done;
    do {
        done = true;
        for (auto& task : tasks) {
            done &= task->isdead();
        }
        if (!done) {
            std::this_thread::sleep_for(5ms);
        }
    } while (!done && steady_clock::now() < deadline);

    if (!done) {
        FAIL() << "Tasks did not finish within timeout";
    }
}

TEST_F(TaskConcurrencyTest, BelowLimit) {
    // test that for N tasks where N < permitted concurrency, tasks never
    // need to wait on the semaphore.

    const auto numTasks = 1;
    const auto permittedConcurrency = 3;

    cb::AwaitableSemaphore semaphore(permittedConcurrency);
    ThreadGate threadsReady(numTasks + 1 /* main test thread */);
    ThreadGate threadsContinue(numTasks + 1 /* main test thread */);

    auto tasks = makeTasks(semaphore, numTasks, [&](auto& guard) {
        // all the tasks should get a token immediately
        EXPECT_TRUE(guard);
        threadsReady.threadUp();
        threadsContinue.threadUp();
    });
    scheduleAll(tasks);

    // wait while all tasks have either acquired a token, or queued for
    // notification.
    threadsReady.threadUp();

    for (const auto& task : tasks) {
        // every task has run once
        EXPECT_EQ(1, task->runs);
        // every task acquired a token
        EXPECT_EQ(1, task->runsAcquiredToken);
    }

    // all the tasks are running, and none are waiting for a token
    EXPECT_EQ(0, semaphore.getWaiters().size());

    // allow tasks to finish
    threadsContinue.threadUp();

    waitForTasks(tasks);
}

TEST_F(TaskConcurrencyTest, AtLimit) {
    // test that for N tasks where N = permitted concurrency, tasks never
    // need to wait on the semaphore.

    const auto numTasks = 3;
    const auto permittedConcurrency = 3;

    cb::AwaitableSemaphore semaphore(permittedConcurrency);
    ThreadGate threadsReady(numTasks + 1 /* main test thread */);
    ThreadGate threadsContinue(numTasks + 1 /* main test thread */);

    std::atomic<int> taskReadyCount = 0;

    auto tasks = makeTasks(semaphore, numTasks, [&](auto& guard) {
        // all the tasks should get a token immediately
        EXPECT_TRUE(guard);
        taskReadyCount++;
        threadsReady.threadUp();
        threadsContinue.threadUp();
    });
    scheduleAll(tasks);

    // wait while all tasks have either acquired a token, or queued for
    // notification.
    threadsReady.threadUp();

    for (const auto& task : tasks) {
        // every task has run once
        EXPECT_EQ(1, task->runs);
        // every task acquired a token
        EXPECT_EQ(1, task->runsAcquiredToken);
    }

    EXPECT_EQ(numTasks, taskReadyCount);

    // all the tasks are running, and none are waiting for a token
    EXPECT_EQ(0, semaphore.getWaiters().size());

    // allow tasks to finish
    threadsContinue.threadUp();

    waitForTasks(tasks);
}

TEST_F(TaskConcurrencyTest, BeyondLimit) {
    // test that for N tasks where N > permitted concurrency, tasks wait on
    // the semaphore, and will later be notified to run.

    const auto numTasks = 4;
    const auto permittedConcurrency = 2;

    cb::AwaitableSemaphore semaphore(permittedConcurrency);
    ThreadGate threadsReady(numTasks + 1 /* main test thread */);
    ThreadGate threadsContinue(numTasks + 1 /* main test thread */);

    std::atomic<int> taskReadyCount = 0;
    std::atomic<int> taskWaitingCount = 0;

    auto tasks = makeTasks(semaphore, numTasks, [&](auto& guard) {
        if (guard) {
            taskReadyCount++;
        } else {
            taskWaitingCount++;
        }
        threadsReady.threadUp();
        threadsContinue.threadUp();
    });
    scheduleAll(tasks);

    // wait while all tasks have either acquired a token, or queued for
    // notification.
    threadsReady.threadUp();

    EXPECT_EQ(permittedConcurrency, taskReadyCount);
    EXPECT_EQ(numTasks - permittedConcurrency, taskWaitingCount);

    {
        // The excess tasks should have been queued for notification
        auto waiters = semaphore.getWaiters();
        EXPECT_EQ(numTasks - permittedConcurrency, waiters.size());

        for (const auto& waiter : waiters) {
            // get a shared ptr
            auto ptr = waiter.lock();
            const auto& task = dynamic_cast<ConcurrencyTestTask&>(*ptr);
            // every task has run once
            EXPECT_EQ(1, task.runs);
            // no waiting task should have acquired a token
            EXPECT_EQ(0, task.runsAcquiredToken);
        }
    }

    // allow tasks to finish
    threadsContinue.threadUp();

    waitForTasks(tasks);

    // now all the tasks have run successfully
    EXPECT_EQ(numTasks, taskReadyCount);
    // no waiters are left
    EXPECT_EQ(0, semaphore.getWaiters().size());
}

TEST_F(TaskConcurrencyTest, BeyondLimitAndThreads) {
    // test that for N tasks where N > permitted concurrency, AND N > number of
    // threads, tasks wait on the semaphore, and will later be notified to run.
    // The tasks can't all try to run at the same time, there's too few threads.

    const auto numThreads = ExecutorPool::get()->getNumNonIO();
    const auto numTasks = numThreads * 2;
    const auto permittedConcurrency = 2;

    cb::AwaitableSemaphore semaphore(permittedConcurrency);

    std::atomic<int> taskReadyCount = 0;
    std::atomic<int> taskWaitingCount = 0;

    // note, we can't block until all the tasks are running, there's too
    // few threads, so that would block forever. Instead, at least ensure
    // multiple do _try_ to run at once, to at least give TSAN a chance
    ThreadGate multipleTasksRunning(numThreads);

    auto tasks = makeTasks(semaphore, numTasks, [&](auto& guard) {
        multipleTasksRunning.threadUp();
        if (guard) {
            taskReadyCount++;
        } else {
            taskWaitingCount++;
        }
    });
    scheduleAll(tasks);

    waitForTasks(tasks);

    // Can't make as tight guarantees for this test. Check that every task
    // did eventually get a guard, and that some tasks did have to wait
    // as
    //  numThreads > permittedConcurrency
    // and multipleTasksRunning ensured numThreads tasks were running
    // at the same time at one point.

    for (const auto& task : tasks) {
        // every task only acquired a token once
        EXPECT_EQ(1, task->runsAcquiredToken);
        // every task has run at least once
        EXPECT_GE(task->runs, 1);
    }

    EXPECT_EQ(numTasks, taskReadyCount);
    EXPECT_GT(taskWaitingCount, 0);

    // no waiters are left
    EXPECT_EQ(0, semaphore.getWaiters().size());
}