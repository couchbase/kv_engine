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

//
// Benchmarks for ExecutorPool / Task scheduling and execution.
//

#include "executorpool.h"
#include "tests/mock/mock_taskable.h"
#include "tests/module_tests/executorpool_test.h"
#include "tests/module_tests/lambda_task.h"

#include <benchmark/benchmark.h>
#include <folly/executors/CPUThreadPoolExecutor.h>
#include <folly/executors/IOThreadPoolExecutor.h>
#include <folly/futures/Future.h>
#include <relaxed_atomic.h>
#include <random>

/**
 * Benchmark fixture using ep-engine's ExecutorPool.
 */
class ExecutorBench : public benchmark::Fixture {
protected:
    void makePool(int nonIOCount) {
        // Note: Don't actually need any Reader/Writer/AuxIO threads here, but
        // ExecutorPool cannot handle having a count of zero :(
        pool = std::make_unique<TestExecutorPool>(
                nonIOCount,
                ThreadPoolConfig::ThreadCount(1),
                ThreadPoolConfig::ThreadCount(1),
                1,
                nonIOCount);
        pool->registerTaskable(taskable);
    }

    void shutdownPool() {
        pool->unregisterTaskable(taskable, false);
        pool.reset();
    }

    std::unique_ptr<TestExecutorPool> pool;
    MockTaskable taskable;
};

/**
 * Benchmark a one-shot task (runs once then completed). Measure how long it
 * takes to:
 * - schedule the Task to run on a background thread (to run asap),
 * - then run that Task and notify back to the main thread.
 *
 * This is the pattern of KVBucket::visitAsync() - it creates a VBCBAdapter Task
 * wrapping the given VBucket visitor object, then runs that task immediately.
 *
 * This is a multithreaded benchmark - N sets of "benchmark" and NonIO threads
 * are created. The benchmark threads repeatedly schedule a Task to be run to
 * perform some work asynchronously, then waits on the result. The background
 * NonIO threads pickup the Task and run it, then notifies the requesting
 * thread via a condition variable.
 *
 * By creating multiple benchmark threads we can measure the performance of the
 * ExecutorPool's scheduling implementation.
 * By creating multiple background NonIO threads we can measure the performance
 * of the actual dispatch of Tasks on a ThreadPool.
 */
BENCHMARK_DEFINE_F(ExecutorBench, OneShotScheduleRun)(benchmark::State& state) {
    if (state.thread_index == 0) {
        // Create one "NonIO" thread per "benchmark" thread (such that there is
        // a pair of benchmark and NonIO threads to consume/produce).
        makePool(state.threads);
    }
    folly::Baton cv;
    cb::RelaxedAtomic<int64_t> producerCount{0};
    int64_t consumerCount = 0;

    // Simple producer function to run in the our Task - increments
    // producerCount and notifies the waiting thread via a condition variable.
    auto producerFn = [&cv, &producerCount, &state] {
        producerCount++;
        cv.post();
        return false;
    };

    // Benchmark loop
    while (state.KeepRunning()) {
        ExTask task = std::make_shared<LambdaTask>(
                taskable, TaskId::ItemPager, 0, true, producerFn);
        pool->schedule(task);
        consumerCount++;
        cv.wait();
        EXPECT_EQ(consumerCount, producerCount.load());
        cv.reset();
    }
    state.SetItemsProcessed(consumerCount);
}

/**
 * Benchmark the performance of scheduling a Task to run in the future,
 * but it is cancelled shortly after (before running).
 *
 * This models the behaviour of a per-VBucket Task to handle SyncWrite timeouts
 * - each Task is scheduled to run when the first tracked SyncWrite will
 * timeout, however that typically never happens as the SyncWrite will complete
 * beforehand and the Task will be re-scheduled to run later (when the next
 * tracked SyncWrite expires.
 *
 * This is a multi-threaded benchmark - each benchmark thread attempts to
 * cancel and reschedule the timeout tasks against a single thread pool.
 *
 * Argumment specifies how many Tasks exist (spread across all threads). As
 * such we can measure how the same number of Tasks are handled when different
 * number of threads are trying to schedule them.
 */
BENCHMARK_DEFINE_F(ExecutorBench, TimeoutAddCancel)(benchmark::State& state) {
    if (state.thread_index == 0) {
        // Create a fixed number of threads (we don't actually expect to ever
        // run anything).
        makePool(1);
    }
    const auto tasksPerThread = state.range(0) / state.threads;
    ASSERT_NE(0, tasksPerThread) << "Must have at least 1 task per thread";

    std::random_device randomDevice;
    std::mt19937_64 generator(randomDevice());

    // Create N Timeout tasks and register with ExecutorPool - runs once
    // if/when the timeout occurs.
    // Schedule them all to run (timeout) a pseudo-random time from 10-30
    // seconds. Don't actually expect this to run in this configuration, given
    // we will cancel it before the timeout.
    std::uniform_real_distribution<> timeoutDistribution(10.0, 30.0);
    std::vector<ExTask> tasks;
    for (int i = 0; i < tasksPerThread; i++) {
        ExTask task = std::make_shared<LambdaTask>(
                taskable,
                TaskId::ItemPager,
                std::numeric_limits<int>::max(),
                true,
                [] {
                    throw std::logic_error("Timeout should never be executed.");
                    return false;
                });
        tasks.push_back(task);
    }

    // Benchmark loop - pick the next Task, cancel it's current timeout and
    // then add a new timeout.
    auto nextTask = tasks.begin();
    int64_t tasksSnoozed = 0;
    while (state.KeepRunning()) {
        if ((*nextTask)->getState() == TASK_SNOOZED) {
            // First time this task has been operated on - schedule it.
            pool->schedule(*nextTask);
        }
        EXPECT_TRUE(pool->snooze((*nextTask)->getId(),
                                 timeoutDistribution(generator)));
        tasksSnoozed++;
        nextTask++;
        if (nextTask == tasks.end()) {
            nextTask = tasks.begin();
        }
    }

    // Cancel all tasks so they don't run during pool shutdown.
    for (auto& t : tasks) {
        t->cancel();
    }

    state.SetItemsProcessed(tasksSnoozed);

    if (state.thread_index == 0) {
        shutdownPool();
    }
}

/**
 * Benchmark fixture using Folly's CPUThreaPoolPoolExecutor &
 * IOThreadPoolExecutor.
 */
class FollyExecutorBench : public benchmark::Fixture {
protected:
    // Test Timeout object.
    struct TestTimeout : public folly::HHWheelTimer::Callback {
        void timeoutExpired() noexcept override {
            expired = true;
        }
        void callbackCanceled() noexcept override {
            cancelled = true;
        }
        bool expired{false};
        bool cancelled{false};
    };

    void makePool(int numCPUThreads, int numIOThreads) {
        // Disable dynamic thread creation / destruction to match ep-engine
        // Executor behaviour (threads created up-front and hang around).
        FLAGS_dynamic_cputhreadpoolexecutor = false;

        pool = std::make_unique<folly::CPUThreadPoolExecutor>(numCPUThreads);
        ioPool = std::make_unique<folly::IOThreadPoolExecutor>(numIOThreads);

        // To match functionality of ep-engine ExecutorPool, register
        // a callback to record task wait/run times.
        // TODO: Make MockTaskable actually record times in histogram
        // as per EPEngine.
        // TODO: Record the timings on a per-Task basis. Folly adds support
        // for this as of
        // https://github.com/facebook/folly/commit/7469e0b55e0d534da34ef6bfe4d0d0068f023cd9
        auto statsCallback = [taskable = &this->taskable](
                                     folly::ThreadPoolExecutor::TaskStats ts) {
            taskable->logQTime(TaskId::ItemPager, ts.waitTime);
            taskable->logRunTime(TaskId::ItemPager, ts.runTime);
        };
        pool->subscribeToTaskStats(statsCallback);
        ioPool->subscribeToTaskStats(statsCallback);
    }

    void shutdownPool() {
        pool.reset();
        ioPool.reset();
    }

    std::unique_ptr<folly::CPUThreadPoolExecutor> pool;
    std::unique_ptr<folly::IOThreadPoolExecutor> ioPool;
    MockTaskable taskable;

    // Collection of TestTimeout tasks, shared across multiple benchmark
    // threds.
    std::vector<TestTimeout> tasks;
};

/**
 * Folly Executor variant of ExecutorBench::OneShotScheduleRun - see
 * that benchmark for description of what is being tested.
 */
BENCHMARK_DEFINE_F(FollyExecutorBench, OneShotScheduleRun)
(benchmark::State& state) {
    if (state.thread_index == 0) {
        // Create one "NonIO" thread per "benchmark" thread (such that there is
        // a pair of benchmark and NonIO threads to consume/produce).
        makePool(state.threads, 0);
    }

    folly::Baton cv;
    cb::RelaxedAtomic<int64_t> producerCount{0};
    int64_t consumerCount = 0;

    // Simple producer function to run in the our Task - increments
    // producerCount and notifies the waiting thread via a condition variable.
    auto producerFn = [&cv, &producerCount, &state] {
        producerCount++;
        cv.post();
    };

    // Benchmark loop
    while (state.KeepRunning()) {
        pool->add(producerFn);
        consumerCount++;
        cv.wait();
        EXPECT_EQ(consumerCount, producerCount.load());
        cv.reset();
    }
    state.SetItemsProcessed(consumerCount);
}

/**
 * Folly Executor variant of ExecutorBench::TimeoutAddCancel - see
 * that benchmark for description of what is being tested.
 */
BENCHMARK_DEFINE_F(FollyExecutorBench, TimeoutAddCancel)
(benchmark::State& state) {
    if (state.thread_index == 0) {
        // Create a single IO thread, giving us a single EventBase +
        // HHWheelTimer to manage the timeouts.
        makePool(0, 1);

        // Create N Timeout tasks and register with ExecutorPool - runs once
        // if/when the timeout occurs.
        tasks.resize(state.range(0));
    }

    std::random_device randomDevice;
    std::mt19937_64 generator(randomDevice());

    // Benchmark loop - pick the next Task, cancel it's current timeout and
    // then add a new timeout.
    // Each thread picks tasks modulo it's threadID.
    // Schedule them all to run (timeout) a pseudo-random time from 10-30
    // seconds. Don't actually expect this to run in this configuration, given
    // we will cancel it before the timeout.
    std::uniform_int_distribution<> timeoutDistribution(10000, 30000);

    size_t taskIndex = state.thread_index;
    int64_t tasksSnoozed = 0;
    while (state.KeepRunning()) {
        auto* eventBase = ioPool->getEventBase();
        auto timeout =
                std::chrono::milliseconds(timeoutDistribution(generator));
        auto* callback = &tasks.at(taskIndex);

        // Request that the eventBase thread re-schedules the timeout.
        // There's two choices here:
        //   a) Use runInEventBaseThread (non-blocking).
        //   b) Use runInEventBaseThreadAndWait (blocking).
        // The non-blocking call is more representative of how this would be
        // used in a typical ep-engine scenario (e.g. SyncWrite timeout),
        // however that doesn't actually measure the cost of updating the
        // HHWheelTimer in the eventBase thread, so arguably gives
        // unrepresentative numbers.
        // The blocking call however has the opposite issues - it's more costly
        // than we would actually see in typical use, but does at least capture
        // the time taken to update the HHWheelTimer.
        // As a compromise, we normally use the non-blocking variant, but on the
        // last iteration we use the blocking one. Given callbacks are run
        // in-order on the EventBaseThread, this forces all previous
        // scheduleTimeout() calls to have completed.
        auto scheduleTimeoutFn = [eventBase, callback, timeout] {
            eventBase->timer().scheduleTimeout(callback, timeout);
        };
        if (state.iterations() < state.max_iterations) {
            eventBase->runInEventBaseThread(scheduleTimeoutFn);
        } else {
            eventBase->runInEventBaseThreadAndWait(scheduleTimeoutFn);
        }

        tasksSnoozed++;
        taskIndex += state.threads;
        if (taskIndex >= tasks.size()) {
            taskIndex = state.thread_index;
        }
    }

    state.SetItemsProcessed(tasksSnoozed);

    if (state.thread_index == 0) {
        // Sanity check - no Callbacks should have been executed.
        for (auto& t : tasks) {
            if (t.expired) {
                throw std::logic_error("Timeout should never be executed.");
            }
        }

        // Cleanup - cancel all outstanding Callbacks (necessary before we
        // destruct the tasks otherwise a callback could fire while tasks are
        // being destroyed).
        auto* eventBase = ioPool->getEventBase();
        eventBase->runInEventBaseThreadAndWait(
                [eventBase] { eventBase->timer().cancelAll(); });

        shutdownPool();
    }
}

BENCHMARK_REGISTER_F(ExecutorBench, OneShotScheduleRun)
        ->ThreadRange(1, 16)
        ->UseRealTime();

BENCHMARK_REGISTER_F(FollyExecutorBench, OneShotScheduleRun)
        ->ThreadRange(1, 16)
        ->UseRealTime();

BENCHMARK_REGISTER_F(ExecutorBench, TimeoutAddCancel)
        ->ThreadRange(1, 16)
        ->Range(1000, 30000)
        ->ArgName("Timeouts")
        ->UseRealTime();

BENCHMARK_REGISTER_F(FollyExecutorBench, TimeoutAddCancel)
        ->ThreadRange(1, 16)
        ->Range(1000, 30000)
        ->ArgName("Timeouts")
        ->UseRealTime();
