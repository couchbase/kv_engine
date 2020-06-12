/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2017 Couchbase, Inc
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

/*
 * Unit tests for the ExecutorPool class
 */

#pragma once

#include "../mock/mock_taskable.h"
#include "executorpool.h"
#include "executorthread.h"
#include "fakes/fake_executorpool.h"
#include "taskable.h"
#include "thread_gate.h"
#include <folly/portability/GTest.h>
#include <thread>

class TestExecutorPool : public CB3ExecutorPool {
public:
    TestExecutorPool(size_t maxThreads,
                     ThreadPoolConfig::ThreadCount maxReaders,
                     ThreadPoolConfig::ThreadCount maxWriters,
                     size_t maxAuxIO,
                     size_t maxNonIO)
        : CB3ExecutorPool(
                  maxThreads, maxReaders, maxWriters, maxAuxIO, maxNonIO) {
    }

    size_t getNumBuckets() {
        return numBuckets;
    }

    std::vector<std::string> getThreadNames() {
        LockHolder lh(tMutex);

        std::vector<std::string> output;

        std::for_each(threadQ.begin(),
                      threadQ.end(),
                      [&output](const CB3ExecutorThread* v) {
                          output.push_back(v->getName());
                      });

        return output;
    }

    // Returns a vector of the registered ExecutorThreads, non-owning.
    // WARNING: Not safe to reduce thread pool size while the result of
    // this method is still in use.
    ThreadQ getThreads() {
        LockHolder lh(tMutex);
        ThreadQ result = threadQ;
        return result;
    }

    bool threadExists(std::string name) {
        auto names = getThreadNames();
        return std::find(names.begin(), names.end(), name) != names.end();
    }

    /** Waits indefinitely for the taskLocator to become empty, indicating all
     * tasks have been cancelled and cleaned up.
     */
    void waitForEmptyTaskLocator() {
        std::unique_lock<std::mutex> lh(tMutex);
        tMutex.wait(lh, [this] { return taskLocator.empty(); });
    }

    ~TestExecutorPool() override = default;
};

class ExecutorPoolTest : public ::testing::Test {};

class SingleThreadedExecutorPoolTest : public ::testing::Test {
public:
    void SetUp() override {
        SingleThreadedExecutorPool::replaceExecutorPoolWithFake();
        pool = ExecutorPool::get();
        pool->registerTaskable(taskable);
    }

    void TearDown() override {
        pool->unregisterTaskable(taskable, false);
        pool->shutdown();
    }

    ExecutorPool* pool;
    MockTaskable taskable;
};

class ExecutorPoolDynamicWorkerTest : public ExecutorPoolTest {
protected:
    // Simulated number of CPUs. Want a value >16 to be able to test
    // the difference between Default and DiskIOOptimized thread counts.
    const size_t MaxThreads{18};

    void SetUp() override {
        ExecutorPoolTest::SetUp();
        pool = std::unique_ptr<TestExecutorPool>(new TestExecutorPool(
                MaxThreads,
                ThreadPoolConfig::ThreadCount(2), // MaxNumReaders
                ThreadPoolConfig::ThreadCount(2), // MaxNumWriters
                2, // MaxNumAuxio
                2 // MaxNumNonio
                ));
        pool->registerTaskable(taskable);
    }

    void TearDown() override {
        pool->unregisterTaskable(taskable, false);
        pool->shutdown();
        ExecutorPoolTest::TearDown();
    }

    std::unique_ptr<TestExecutorPool> pool;

    /* ThreadGate may still be in use in threads after a test has completed,
     * must be kept around until after the taskable is unregistered and the
     * tasks are certain to have ended, hence its declaration here but
     * assignment in each test case - if it were declared in the scope of
     * the test it would be deleted before the taskable is unregistered.
     */
    std::unique_ptr<ThreadGate> tg;
    MockTaskable taskable;
};

struct ThreadCountsParams {
    // Input params:
    ThreadPoolConfig::ThreadCount in_reader_writer;
    size_t maxThreads;

    // Expected outputs:
    size_t reader;
    size_t writer;
    size_t auxIO;
    size_t nonIO;
};

::std::ostream& operator<<(::std::ostream& os,
                           const ThreadCountsParams& expected);

class ExecutorPoolTestWithParam
    : public ExecutorPoolTest,
      public ::testing::WithParamInterface<ThreadCountsParams> {};
