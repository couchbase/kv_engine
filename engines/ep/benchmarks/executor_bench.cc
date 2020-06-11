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
#include <folly/futures/Future.h>

/**
 * Benchmark fixture using ep-engine's ExecutorPool.
 */
class ExecutorBench : public benchmark::Fixture {
protected:
    void SetUp(::benchmark::State& st) override {
        if (st.thread_index == 0) {
            // Create one NonIO thread per "benchmark" thread (such that
            // there is a pair of benchmark and NonIO threads to
            // consume/produce. Note: Don't actually need any
            // Reader/Writer/AuxIO threads here, but ExecutorPool cannot handle
            // having a count of zero :(
            pool = std::make_unique<TestExecutorPool>(
                    st.threads,
                    NUM_TASK_GROUPS,
                    ThreadPoolConfig::ThreadCount(1),
                    ThreadPoolConfig::ThreadCount(1),
                    1,
                    st.threads);
            pool->registerTaskable(taskable);
        }
    }

    void TearDown(::benchmark::State& st) override {
        if (st.thread_index == 0) {
            pool->unregisterTaskable(taskable, false);
            pool.reset();
        }
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
 * Benchmark fixture using Folly's CPUThreaPoolPoolExecutor.
 */
class FollyExecutorBench : public benchmark::Fixture {
protected:
    void SetUp(::benchmark::State& st) override {
        if (st.thread_index == 0) {
            // Disable dynamic thread creation / destruction to match ep-engine
            // Executor behaviour (threads created up-front and hang around).
            FLAGS_dynamic_cputhreadpoolexecutor = false;

            // Create one "NonIO" thread per "benchmark" thread (such that
            // there is a pair of benchmark and NonIO threads to
            // consume/produce.
            pool = std::make_unique<folly::CPUThreadPoolExecutor>(st.threads);

            // To match functionality of ep-engine ExecutorPool, register
            // a callback to record task wait/run times.
            // TODO: Make MockTaskable actually record times in histogram
            // as per EPEngine.
            // TODO: Record the timings on a per-Task basis. Folly adds support
            // for this as of
            // https://github.com/facebook/folly/commit/7469e0b55e0d534da34ef6bfe4d0d0068f023cd9
            pool->subscribeToTaskStats(
                    [taskable = &this->taskable](
                            folly::ThreadPoolExecutor::TaskStats ts) {
                        taskable->logQTime(TaskId::ItemPager, ts.waitTime);
                        taskable->logRunTime(TaskId::ItemPager, ts.runTime);
                    });
        }
    }

    void TearDown(::benchmark::State& st) override {
        if (st.thread_index == 0) {
            pool.reset();
        }
    }

    std::unique_ptr<folly::CPUThreadPoolExecutor> pool;
    MockTaskable taskable;
};

/**
 * Folly Executor variant of ExecutorBench::OneShotScheduleRun - see
 * that benchmark for description of what is being tested.
 */
BENCHMARK_DEFINE_F(FollyExecutorBench, OneShotScheduleRun)
(benchmark::State& state) {
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

BENCHMARK_REGISTER_F(ExecutorBench, OneShotScheduleRun)
        ->ThreadRange(1, 16)
        ->UseRealTime();

BENCHMARK_REGISTER_F(FollyExecutorBench, OneShotScheduleRun)
        ->ThreadRange(1, 16)
        ->UseRealTime();
