/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2020-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

//
// Benchmarks for ExecutorPool / Task scheduling and execution.
//

#include "tests/mock/mock_taskable.h"
#include "tests/module_tests/executorpool_test.h"
#include "tests/module_tests/lambda_task.h"
#include <benchmark/benchmark.h>
#include <executor/executorpool.h>
#include <executor/folly_executorpool.h>
#include <folly/executors/CPUThreadPoolExecutor.h>
#include <folly/executors/IOThreadPoolExecutor.h>
#include <folly/futures/Future.h>
#include <mock/mock_global_task.h>
#include <relaxed_atomic.h>
#include <random>

/**
 * Implementation of Taskable which does absolutely nothing. Used in the below
 * benchmarks to provide a light(est) weight Taskable - note that MockTaskable
 * uses GoogleMock for mocking some methods and that adds non-trivial overhead.
 */
class NullTaskable : public Taskable {
public:
    const std::string& getName() const override {
        return name;
    }
    task_gid_t getGID() const override {
        return 0;
    }
    bucket_priority_t getWorkloadPriority() const override {
        return HIGH_BUCKET_PRIORITY;
    }
    void setWorkloadPriority(bucket_priority_t prio) override {
    }
    WorkLoadPolicy& getWorkLoadPolicy() override {
        return policy;
    }
    void logQTime(const GlobalTask& task,
                  std::string_view threadName,
                  std::chrono::steady_clock::duration enqTime) override {
    }
    void logRunTime(const GlobalTask& task,
                    std::string_view threadName,
                    std::chrono::steady_clock::duration runTime) override {
    }
    bool isShutdown() const override {
        return false;
    }

private:
    std::string name{"NullTaskable"};
    WorkLoadPolicy policy{HIGH_BUCKET_PRIORITY, 1};
};

/**
 * Benchmark fixture for subclasses of ep-engine's ExecutorPool class.
 */
template <typename T>
class ExecutorPoolFixture : public benchmark::Fixture {
public:
    /// Setup the ExecutorPool. After this method returns, the calling
    // thread is safe to use `pool`.
    void setupPool(const benchmark::State& state, int nonIOCount) {
        if (state.thread_index() == 0) {
            // Create the pool with the specified number of IO threads.
            // Note: Don't actually need any Reader/Writer/AuxIO threads here,
            // but ExecutorPool cannot handle having a count of zero :(
            pool = std::make_unique<T>(nonIOCount,
                                       ThreadPoolConfig::ThreadCount(1),
                                       ThreadPoolConfig::ThreadCount(1),
                                       1,
                                       nonIOCount);
            pool->registerTaskable(taskable);
            poolPtr.store(pool.get());
            poolSem.post();
        } else {
            // Wait for pool to be created by thread 0. Atherwise when we
            // attempt to use the pool before the KeepRunning() loop it may not
            // exist yet.
            poolSem.wait();
        }
    }

    void shutdownPool(const benchmark::State& state) {
        if (state.thread_index() == 0) {
            poolSem.reset();
            poolPtr.store(nullptr);
            pool->unregisterTaskable(taskable, false);
            pool.reset();
        }
    }

    T* getPool() {
        return poolPtr.load();
    }

    /**
     * Benchmark a one-shot task (runs once then completed). Measure how long it
     * takes to:
     * - schedule the Task to run on a background thread (to run asap),
     * - then run that Task and notify back to the main thread.
     *
     * This is the pattern of KVBucket::visitAsync() - it creates a VBCBAdapter
     * Task wrapping the given VBucket visitor object, then runs that task
     * immediately.
     *
     * This is a multithreaded benchmark - N sets of "benchmark" and NonIO
     * threads are created. The benchmark threads repeatedly schedule a Task to
     * be run to perform some work asynchronously, then waits on the result. The
     * background NonIO threads pickup the Task and run it, then notifies the
     * requesting thread via a condition variable.
     *
     * By creating multiple benchmark threads we can measure the performance of
     * the ExecutorPool's scheduling implementation. By creating multiple
     * background NonIO threads we can measure the performance of the actual
     * dispatch of Tasks on a ThreadPool.
     */
    void bench_OneShotScheduleRun(benchmark::State& state) {
        // Create one "NonIO" thread per "benchmark" thread (such that there
        // is a pair of benchmark and NonIO threads to consume/produce).
        setupPool(state, state.threads());

        folly::Baton cv;
        cb::RelaxedAtomic<int64_t> producerCount{0};
        int64_t consumerCount = 0;

        // Simple producer function to run in the our Task - increments
        // producerCount and notifies the waiting thread via a condition
        // variable.
        auto producerFn = [&cv, &producerCount, &state](LambdaTask&) {
            producerCount++;
            cv.post();
            return false;
        };

        // Benchmark loop
        while (state.KeepRunning()) {
            ExTask task = std::make_shared<LambdaTask>(
                    taskable, TaskId::ItemPager, 0, true, producerFn);
            getPool()->schedule(task);
            consumerCount++;
            cv.wait();
            EXPECT_EQ(consumerCount, producerCount.load());
            cv.reset();
        }
        state.SetItemsProcessed(consumerCount);

        shutdownPool(state);
    }

    /**
     * Benchmark a long-lived task (runs once, then sleeps, then run again...).
     * Measure how long it
     * takes to:
     * - wake the Task to run on a background thread (to run asap),
     * - then run that Task and notify back to the main thread.
     *
     * This is the pattern of the Flusher - it creates a Flusher task per shard,
     * then runs that task whenever there is pending items to flush for a
     * vBucket in that shard.
     *
     * This is a multithreaded benchmark - N sets of "benchmark" and NonIO
     * threads are created. The benchmark threads repeatedly wake a Task to
     * be run to perform some work asynchronously, then waits on the result. The
     * background NonIO threads pickup the Task and run it, then notifies the
     * requesting thread via a condition variable.
     *
     * By creating multiple benchmark threads we can measure the performance of
     * the ExecutorPool's scheduling implementation. By creating multiple
     * background NonIO threads we can measure the performance of the actual
     * dispatch of Tasks on a ThreadPool.
     */
    void bench_LongLivedScheduleRun(benchmark::State& state) {
        // Create one "NonIO" thread per "benchmark" thread (such that there
        // is a pair of benchmark and NonIO threads to consume/produce).
        setupPool(state, state.threads());

        folly::Baton cv;
        cb::RelaxedAtomic<int64_t> producerCount{0};
        int64_t consumerCount = 0;

        // Simple producer function to run in the our Task - increments
        // producerCount and notifies the waiting thread via a condition
        // variable.
        auto producerFn = [&cv, &producerCount, &state](LambdaTask& task) {
            task.snooze(INT_MAX);
            producerCount++;
            cv.post();
            return true;
        };

        // Create and schedule N / Nthreads tasks (initial sleep time is
        // forever).
        const auto tasksPerThread = state.range(0) / state.threads();
        std::vector<ExTask> tasks;
        for (int i = 0; i < tasksPerThread; i++) {
            tasks.push_back(std::make_shared<LambdaTask>(
                    taskable, TaskId::ItemPager, INT_MAX, false, producerFn));
            getPool()->schedule(tasks.back());
        }

        // Benchmark loop - repeatedly wake one of our tasks and wait for it
        // to run.
        auto nextTask = tasks.begin();
        while (state.KeepRunning()) {
            getPool()->wake((*nextTask)->getId());
            consumerCount++;
            cv.wait();
            EXPECT_EQ(consumerCount, producerCount.load());
            cv.reset();
            nextTask++;
            if (nextTask == tasks.end()) {
                nextTask = tasks.begin();
            }
        }

        // Cancel all tasks so they don't run during pool shutdown.
        for (auto& t : tasks) {
            t->cancel();
        }

        state.SetItemsProcessed(consumerCount);

        shutdownPool(state);
    }

    /**
     * Benchmark the performance of scheduling a Task to run in the future,
     * but it is cancelled shortly after (before running).
     *
     * This models the behaviour of a per-VBucket Task to handle SyncWrite
     * timeouts
     * - each Task is scheduled to run when the first tracked SyncWrite will
     * timeout, however that typically never happens as the SyncWrite will
     * complete beforehand and the Task will be re-scheduled to run later (when
     * the next tracked SyncWrite expires.
     *
     * This is a multi-threaded benchmark - each benchmark thread attempts to
     * cancel and reschedule the timeout tasks against a single thread pool.
     *
     * Argumment specifies how many Tasks exist (spread across all threads). As
     * such we can measure how the same number of Tasks are handled when
     * different number of threads are trying to schedule them.
     */
    void bench_TimeoutAddCancel(benchmark::State& state) {
        // Create a fixed number of threads (we don't actually expect to
        // ever run anything).
        setupPool(state, 1);

        const auto tasksPerThread = state.range(0) / state.threads();
        ASSERT_NE(0, tasksPerThread) << "Must have at least 1 task per thread";

        std::random_device randomDevice;
        std::mt19937_64 generator(randomDevice());

        // Create N Timeout tasks and register with ExecutorPool - runs once
        // if/when the timeout occurs.
        // Schedule them all to run (timeout) a pseudo-random time from 10-30
        // seconds. Don't actually expect this to run in this configuration,
        // given we will cancel it before the timeout.
        std::uniform_real_distribution<> timeoutDistribution(10.0, 30.0);
        std::vector<ExTask> tasks;
        for (int i = 0; i < tasksPerThread; i++) {
            ExTask task = std::make_shared<LambdaTask>(
                    taskable,
                    TaskId::ItemPager,
                    std::numeric_limits<int>::max(),
                    true,
                    [](LambdaTask&) { return false; });
            getPool()->schedule(task);
            tasks.push_back(task);
        }

        // Benchmark loop - pick the next Task, cancel it's current timeout and
        // then add a new timeout.
        auto nextTask = tasks.begin();
        int64_t tasksSnoozed = 0;
        while (state.KeepRunning()) {
            getPool()->snooze((*nextTask)->getId(),
                              timeoutDistribution(generator));
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

        shutdownPool(state);
    }

private:
    std::unique_ptr<T> pool;
    /// Semaphore used to coordinate pool creation/usage.
    folly::SaturatingSemaphore<true> poolSem;

    /// Thread-safe pointer to pool so threads other than the one which
    /// created the pool can correctly access it.
    std::atomic<T*> poolPtr;

    NullTaskable taskable;
};

BENCHMARK_TEMPLATE_DEFINE_F(ExecutorPoolFixture,
                            OneShotScheduleRun_CB3,
                            CB3ExecutorPool)
(benchmark::State& state) {
    bench_OneShotScheduleRun(state);
}

BENCHMARK_TEMPLATE_DEFINE_F(ExecutorPoolFixture,
                            OneShotScheduleRun_Folly,
                            FollyExecutorPool)
(benchmark::State& state) {
    bench_OneShotScheduleRun(state);
}

BENCHMARK_TEMPLATE_DEFINE_F(ExecutorPoolFixture,
                            LongLivedScheduleRun_CB3,
                            CB3ExecutorPool)
(benchmark::State& state) {
    bench_LongLivedScheduleRun(state);
}

BENCHMARK_TEMPLATE_DEFINE_F(ExecutorPoolFixture,
                            LongLivedScheduleRun_Folly,
                            FollyExecutorPool)
(benchmark::State& state) {
    bench_LongLivedScheduleRun(state);
}

BENCHMARK_TEMPLATE_DEFINE_F(ExecutorPoolFixture,
                            TimeoutAddCancel_CB3,
                            CB3ExecutorPool)
(benchmark::State& state) {
    bench_TimeoutAddCancel(state);
}

BENCHMARK_TEMPLATE_DEFINE_F(ExecutorPoolFixture,
                            TimeoutAddCancel_Folly,
                            FollyExecutorPool)
(benchmark::State& state) {
    bench_TimeoutAddCancel(state);
}

/**
 * Benchmark fixture using Folly's CPUThreadPoolPoolExecutor &
 * IOThreadPoolExecutor directly (without any higher-level GlobalTask
 * abstraction).
 */
class PureFollyExecutorBench : public benchmark::Fixture {
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

        dummyTask =
                std::make_shared<MockGlobalTask>(taskable, TaskId::ItemPager);

        // To match functionality of ep-engine ExecutorPool, register
        // a callback to record task wait/run times.
        // TODO: Make taskable actually record times in histogram
        // as per EPEngine.
        // TODO: Record the timings on a per-Task basis. Folly adds support
        // for this as of
        // https://github.com/facebook/folly/commit/7469e0b55e0d534da34ef6bfe4d0d0068f023cd9
        auto statsCallback = [dummyTask = this->dummyTask,
                              taskable = &this->taskable](
                                     folly::ThreadPoolExecutor::TaskStats ts) {
            taskable->logQTime(*dummyTask,
                               folly::getCurrentThreadName().value_or(
                                       "Unknown PureFollyExecutorBench thread"),
                               ts.waitTime);
            taskable->logRunTime(
                    *dummyTask,
                    folly::getCurrentThreadName().value_or(
                            "Unknown PureFollyExecutorBench thread"),
                    ts.runTime);
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
    NullTaskable taskable;

    // Dummy Global task used for statsCallback in makePool.
    ExTask dummyTask;

    // Collection of TestTimeout tasks, shared across multiple benchmark
    // threds.
    std::vector<TestTimeout> tasks;
};

/**
 * Folly Executor variant of CB3ExecutorPoolBench::OneShotScheduleRun - see
 * that benchmark for description of what is being tested.
 */
BENCHMARK_DEFINE_F(PureFollyExecutorBench, OneShotScheduleRun)
(benchmark::State& state) {
    if (state.thread_index() == 0) {
        // Create one "NonIO" thread per "benchmark" thread (such that there is
        // a pair of benchmark and NonIO threads to consume/produce).
        makePool(state.threads(), 0);
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

    if (state.thread_index() == 0) {
        shutdownPool();
    }
}

/**
 * Folly Executor variant of CB3ExecutorPoolBench::TimeoutAddCancel - see
 * that benchmark for description of what is being tested.
 */
BENCHMARK_DEFINE_F(PureFollyExecutorBench, TimeoutAddCancel)
(benchmark::State& state) {
    if (state.thread_index() == 0) {
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

    size_t taskIndex = state.thread_index();
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
        taskIndex += state.threads();
        if (taskIndex >= tasks.size()) {
            taskIndex = state.thread_index();
        }
    }

    state.SetItemsProcessed(tasksSnoozed);

    if (state.thread_index() == 0) {
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

BENCHMARK_REGISTER_F(ExecutorPoolFixture, OneShotScheduleRun_CB3)
        ->ThreadRange(1, 16)
        ->UseRealTime();
BENCHMARK_REGISTER_F(ExecutorPoolFixture, OneShotScheduleRun_Folly)
        ->ThreadRange(1, 16)
        ->UseRealTime();

BENCHMARK_REGISTER_F(PureFollyExecutorBench, OneShotScheduleRun)
        ->ThreadRange(1, 16)
        ->UseRealTime();

BENCHMARK_REGISTER_F(ExecutorPoolFixture, LongLivedScheduleRun_CB3)
        ->ThreadRange(1, 16)
        ->Range(16, 64)
        ->ArgName("Tasks")
        ->UseRealTime();
BENCHMARK_REGISTER_F(ExecutorPoolFixture, LongLivedScheduleRun_Folly)
        ->ThreadRange(1, 16)
        ->Range(16, 64)
        ->ArgName("Tasks")
        ->UseRealTime();

BENCHMARK_REGISTER_F(ExecutorPoolFixture, TimeoutAddCancel_CB3)
        ->ThreadRange(1, 16)
        ->Range(1000, 30000)
        ->ArgName("Timeouts")
        ->UseRealTime();
BENCHMARK_REGISTER_F(ExecutorPoolFixture, TimeoutAddCancel_Folly)
        ->ThreadRange(1, 16)
        ->Range(1000, 30000)
        ->ArgName("Timeouts")
        ->UseRealTime();

BENCHMARK_REGISTER_F(PureFollyExecutorBench, TimeoutAddCancel)
        ->ThreadRange(1, 16)
        ->Range(1000, 30000)
        ->ArgName("Timeouts")
        ->UseRealTime();
