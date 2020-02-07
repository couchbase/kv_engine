/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2015 Couchbase, Inc.
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

#include <daemon/executor.h>
#include <daemon/executorpool.h>
#include <daemon/task.h>
#include <daemon/tracing.h>
#include <daemon/tracing_types.h>
#include <folly/portability/GMock.h>
#include <folly/portability/GTest.h>
#include <phosphor/phosphor.h>
#include <platform/backtrace.h>
#include <atomic>
#include <memory>

class ExecutorTest : public ::testing::Test {
protected:
    void SetUp() {
        executorpool = std::make_unique<cb::ExecutorPool>(4);
    }

    std::unique_ptr<cb::ExecutorPool> executorpool;
};

/**
 * Lets create a Task that just increments an integer and signals a
 * condition variable so that we can communicate with another thread.
 */
class BasicTestTask : public Task {
public:
    BasicTestTask(int max_)
        : Task(),
          runcount(0),
          max(max_) {
        executionComplete.store(false);
    }

    virtual ~BasicTestTask() {

    }

    Status execute() override {
        ++runcount;
        if (runcount < max) {
            cond.notify_one();
            return Status::Continue;
        }

        return Status::Finished;
    }


    void notifyExecutionComplete() override {
        executionComplete.store(true);
        cond.notify_one();
    }

    int runcount;
    int max;
    std::condition_variable cond;
    std::atomic_bool executionComplete;
};

TEST_F(ExecutorTest, SingleExecution) {
    auto* cmd = new BasicTestTask(1);

    std::shared_ptr<Task> task(cmd);

    std::unique_lock<std::mutex> lock(task->getMutex());
    executorpool->schedule(task);

    // Note: cannot verify if the runq size is 1 here
    // because the executor pool removes the task from
    // the queue before acquiring its lock.
    cmd->cond.wait(lock);
    EXPECT_EQ(0, executorpool->runqSize());
    EXPECT_EQ(1, cmd->runcount);
    EXPECT_TRUE(cmd->executionComplete);
}

TEST_F(ExecutorTest, MultipleExecution) {
    auto* cmd = new BasicTestTask(10);

    std::shared_ptr<Task> task(cmd);

    std::unique_lock<std::mutex> lock(task->getMutex());
    executorpool->schedule(task);

    for (int ii = 1; ii < cmd->max; ++ii) {
        EXPECT_EQ(0, executorpool->waitqSize());
        cmd->cond.wait(lock);
        EXPECT_EQ(0, executorpool->runqSize());
        EXPECT_EQ(1, executorpool->waitqSize());
        EXPECT_EQ(ii, cmd->runcount);
        cmd->makeRunnable();
    }

    cmd->cond.wait(lock);
    EXPECT_EQ(cmd->max, cmd->runcount);
    EXPECT_TRUE(cmd->executionComplete);
}

TEST_F(ExecutorTest, ScheduleMissingLock) {
    auto* cmd = new BasicTestTask(1);
    std::shared_ptr<Task> task(cmd);
    EXPECT_THROW(executorpool->schedule(task), std::logic_error);
}

TEST_F(ExecutorTest, RescheduleMissingLock) {
    auto* cmd = new BasicTestTask(2);
    std::shared_ptr<Task> task(cmd);
    std::unique_lock<std::mutex> lock(task->getMutex());
    executorpool->schedule(task);

    cmd->cond.wait(lock);
    EXPECT_EQ(1, cmd->runcount);

    lock.unlock();
    EXPECT_THROW(cmd->makeRunnable(), std::logic_error);

    lock.lock();
    cmd->makeRunnable();
    cmd->cond.wait(lock);
    EXPECT_EQ(2, cmd->runcount);
    EXPECT_TRUE(cmd->executionComplete);
}

struct MockProcessClockSource : cb::ProcessClockSource {
    MOCK_METHOD0(now, std::chrono::steady_clock::time_point());
};

/*
 * This test creates a task and schedules it in the future,
 * it runs ExecutorPool::ClockTick and verifies that since no
 * time has passed that the task should not have moved from the
 * futureq to the runq.
 *
 * It then moves the time forward to the time that the task should
 * begin executing. It runs ExecutorPool::ClockTick again and
 * verifies that it has been moved from the futureq and subsequently
 * run.
 */
TEST_F(ExecutorTest, FutureExecution) {
    using namespace testing;

    MockProcessClockSource mockClock;
    auto now = std::chrono::steady_clock::now();

    executorpool = std::make_unique<cb::ExecutorPool>(4, mockClock);

    auto cmd = std::make_shared<BasicTestTask>(1);
    std::shared_ptr<Task> task = cmd;

    std::unique_lock<std::mutex> lock(task->getMutex());
    executorpool->schedule(task, false);

    auto taskTime = now + std::chrono::seconds(5);
    task->makeRunnable(taskTime);

    // Clock source will just return now. The source should be called
    // once per executor (ie. 4 times).
    EXPECT_CALL(mockClock, now()).Times(AtLeast(4)).WillRepeatedly(Return(now));

    EXPECT_EQ(1, executorpool->waitqSize());
    EXPECT_EQ(1, executorpool->futureqSize());

    // can't hold lock for clock tick
    lock.unlock();
    executorpool->clockTick();
    lock.lock();

    EXPECT_EQ(1, executorpool->waitqSize());
    EXPECT_EQ(1, executorpool->futureqSize());

    // Clock source returns the time the task executes
    EXPECT_CALL(mockClock, now())
            .Times(AtLeast(4))
            .WillRepeatedly(Return(taskTime));

    lock.unlock();
    executorpool->clockTick();
    lock.lock();

    EXPECT_EQ(0, executorpool->waitqSize());
    EXPECT_EQ(0, executorpool->futureqSize());

    // because we weren't holding the lock after the task was made
    // runnable then the executor might have already run it.
    if (!cmd->executionComplete) {
        cmd->cond.wait(lock);
    }

    EXPECT_EQ(1, cmd->runcount);
    EXPECT_TRUE(cmd->executionComplete);
}

// This is essentially a periodic version of BasicTestTask above
class PeriodicBasicTestTask : public PeriodicTask {
public:
    /**
     * @param max_ Maximum number of times for the task to run
     */
    PeriodicBasicTestTask(int max_)
        : PeriodicTask(std::chrono::seconds(1)), runcount(0), max(max_) {
        executionComplete.store(false);
    }

    Status periodicExecute() override {
        ++runcount;
        if (runcount < max) {
            cond.notify_one();
            return Status::Continue;
        }

        return Status::Finished;
    }

    void notifyExecutionComplete() override {
        executionComplete.store(true);
        cond.notify_one();
    }

    int runcount;
    int max;
    std::condition_variable cond;
    std::atomic_bool executionComplete;
};

/*
 * This test creates a periodic task with a 1 second period
 * and verifies the executor queue states as time is moved forwards
 */
TEST_F(ExecutorTest, PeriodicExecution) {
    using namespace testing;

    MockProcessClockSource mockClock;
    auto now = std::chrono::steady_clock::now();

    executorpool = std::make_unique<cb::ExecutorPool>(4, mockClock);

    auto cmd = std::make_shared<PeriodicBasicTestTask>(5);
    std::shared_ptr<Task> task = cmd;

    std::unique_lock<std::mutex> lock(task->getMutex());
    executorpool->schedule(task, false);
    task->makeRunnable(std::chrono::steady_clock::now());

    for (int i = 1; i < 6; ++i) {
        // Move 1 second into the future on each iteration
        EXPECT_CALL(mockClock, now())
                .Times(AtLeast(4))
                .WillRepeatedly(Return(now + std::chrono::seconds(i)));

        EXPECT_EQ(1, executorpool->waitqSize());
        EXPECT_EQ(1, executorpool->futureqSize());

        // can't hold lock for clock tick
        lock.unlock();
        executorpool->clockTick();
        lock.lock();

        // because we weren't holding the lock after the task was made
        // runnable then the executor might have already run it.
        if (cmd->runcount != i) {
            // Not ideal that we're only testing this if the executor
            // didn't already get around to running it already but
            // there's not a lot that can be done about that without
            // bloating the executor implementation with external locking
            EXPECT_EQ(0, executorpool->waitqSize());
            EXPECT_EQ(0, executorpool->futureqSize());

            cmd->cond.wait(lock);
        }

        EXPECT_EQ(i, cmd->runcount);
    }

    EXPECT_EQ(0, executorpool->waitqSize());
    EXPECT_EQ(0, executorpool->futureqSize());

    executorpool.reset();

    EXPECT_TRUE(cmd->executionComplete);
}

using StaleTraceDumpRemoverTest = ExecutorTest;

/**
 * Extension of StaleTraceDumpRemover that exposes a condition variable
 * to allow for notification when the task has been executed.
 */
class NotifiableStaleTraceDumpRemover : public StaleTraceDumpRemover {
public:
    using StaleTraceDumpRemover::StaleTraceDumpRemover;

    Status periodicExecute() override {
        Status r = StaleTraceDumpRemover::periodicExecute();
        cond.notify_one();
        ++runcount;
        return r;
    }
    std::condition_variable cond;
    int runcount = 0;
};

/*
 * This test verifies the behaviour of the StaleTraceDumpRemover as a unit test
 * as a testapp test is impractical (since the actual SDR takes 5 minutes
 * to remove a stale dump and there's no time travel).
 */
TEST_F(StaleTraceDumpRemoverTest, DoesRemove) {
    using namespace testing;

    MockProcessClockSource mockClock;
    executorpool = std::make_unique<cb::ExecutorPool>(4, mockClock);

    // Create a trace dump remover task that runs every second and remove
    // dumps that are older than one second from a function local map.
    TraceDumps traceDumps;
    auto cmd = std::make_shared<NotifiableStaleTraceDumpRemover>(
            traceDumps, std::chrono::seconds(1), std::chrono::seconds(1));
    std::shared_ptr<Task> task = cmd;

    std::unique_lock<std::mutex> lock(task->getMutex());
    executorpool->schedule(task, false);

    // Fastest way to create a TraceContext is to start tracing, stop it, and
    // pull out the context
    PHOSPHOR_INSTANCE.start(
            phosphor::TraceConfig(phosphor::BufferMode::ring, 1024 * 1024));
    PHOSPHOR_INSTANCE.stop();
    auto context = PHOSPHOR_INSTANCE.getTraceContext();

    cb::uuid::uuid_t uuid = cb::uuid::random();
    {
        std::lock_guard<std::mutex> lh(traceDumps.mutex);
        traceDumps.dumps.emplace(
                uuid,
                std::make_unique<DumpContext>(
                        phosphor::TraceContext(std::move(context))));
    }

    auto now = traceDumps.dumps.begin()->second->last_touch;
    auto removeTime = now + std::chrono::seconds(1);
    task->makeRunnable(now);

    EXPECT_CALL(mockClock, now()).Times(AtLeast(4)).WillRepeatedly(Return(now));
    lock.unlock();
    executorpool->clockTick();
    lock.lock();

    // It's necessary to check the run count as the task might have already
    // executed. There's no race in checking it as the task won't execute
    // while we hold the task lock.
    if (cmd->runcount != 1) {
        cmd->cond.wait(lock);
    }

    {
        std::lock_guard<std::mutex> lh(traceDumps.mutex);
        EXPECT_EQ(1, traceDumps.dumps.size());
    }

    EXPECT_CALL(mockClock, now())
            .Times(AtLeast(4))
            .WillRepeatedly(Return(removeTime));
    lock.unlock();
    executorpool->clockTick();
    lock.lock();

    if (cmd->runcount != 2) {
        cmd->cond.wait(lock);
    }

    {
        std::lock_guard<std::mutex> lh(traceDumps.mutex);
        EXPECT_EQ(1, traceDumps.dumps.size());
    }
}

static std::terminate_handler default_terminate_handler;

static void my_terminate_handler() {
    char callstack[1024];

    if (print_backtrace_to_buffer("    ", callstack, sizeof(callstack))) {
        std::cerr << "*** Fatal error ***" << std::endl
                  << "Call stack:" << std::endl
                  << callstack;
    } else {
        std::cerr << "*** Falal error, but failed to grab callstack"
                  << std::endl;
    }

    if (default_terminate_handler != nullptr) {
        default_terminate_handler();
    }

    _exit(EXIT_FAILURE);
}

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);

    // Interpose our own C++ terminate handler to print backtrace upon failures
    default_terminate_handler = std::set_terminate(my_terminate_handler);

    return RUN_ALL_TESTS();
}
