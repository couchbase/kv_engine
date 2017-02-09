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

#include <executorpool.h>
#include <executorthread.h>
#include <gtest/gtest.h>
#include <taskable.h>
#include "thread_gate.h"

class MockTaskable : public Taskable {
public:
    MockTaskable();

    const std::string& getName() const;

    task_gid_t getGID() const;

    bucket_priority_t getWorkloadPriority() const;

    void setWorkloadPriority(bucket_priority_t prio);

    WorkLoadPolicy& getWorkLoadPolicy(void);

    void logQTime(TaskId id, const ProcessClock::duration enqTime);

    void logRunTime(TaskId id, const ProcessClock::duration runTime);

protected:
    std::string name;
    WorkLoadPolicy policy;
};

class TestExecutorPool : public ExecutorPool {
public:
    TestExecutorPool(size_t maxThreads,
                     size_t nTaskSets,
                     size_t maxReaders,
                     size_t maxWriters,
                     size_t maxAuxIO,
                     size_t maxNonIO)
        : ExecutorPool(maxThreads,
                       nTaskSets,
                       maxReaders,
                       maxWriters,
                       maxAuxIO,
                       maxNonIO) {
    }

    size_t getNumBuckets() {
        return numBuckets;
    }

    std::vector<std::string> getThreadNames() {
        LockHolder lh(tMutex);

        std::vector<std::string> output;

        std::for_each(threadQ.begin(),
                      threadQ.end(),
                      [&output](const ExecutorThread* v) {
                          output.push_back(v->getName());
                      });

        return output;
    }

    bool threadExists(std::string name) {
        auto names = getThreadNames();
        return std::find(names.begin(), names.end(), name) != names.end();
    }

    ~TestExecutorPool() = default;
};

class ExecutorPoolTest : public ::testing::Test {};

class ExecutorPoolDynamicWorkerTest : public ExecutorPoolTest {
protected:
    void SetUp() override {
        ExecutorPoolTest::SetUp();
        pool = std::unique_ptr<TestExecutorPool>(new TestExecutorPool(
                10, // MaxThreads
                NUM_TASK_GROUPS,
                2, // MaxNumReaders
                2, // MaxNumWriters
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

struct ExpectedThreadCounts {
    size_t maxThreads;
    size_t reader;
    size_t writer;
    size_t auxIO;
    size_t nonIO;
};

::std::ostream& operator<<(::std::ostream& os,
                           const ExpectedThreadCounts& expected) {
    return os << "CPU" << expected.maxThreads << "_W" << expected.writer << "_R"
              << expected.reader << "_A" << expected.auxIO << "_N"
              << expected.nonIO;
}

class ExecutorPoolTestWithParam
        : public ExecutorPoolTest,
          public ::testing::WithParamInterface<ExpectedThreadCounts> {};
