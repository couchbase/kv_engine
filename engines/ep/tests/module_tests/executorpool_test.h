/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2017-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

/*
 * Unit tests for the ExecutorPool class
 */

#pragma once

#include "../mock/mock_synchronous_ep_engine.h"
#include "../mock/mock_taskable.h"
#include "thread_gate.h"
#include <executor/executorpool.h>
#include <executor/fake_executorpool.h>
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

    ~TestExecutorPool() override = default;
};

template <typename T>
class ExecutorPoolTest : public ::testing::Test {
protected:
    void makePool(int maxThreads,
                  int numReaders = 2,
                  int numWriters = 2,
                  int numAuxIO = 2,
                  int numNonIO = 2);

    std::unique_ptr<T> pool;
};

/**
 * Test fixture for ExecutorPool tests which require a full EPEngine instance.
 * @tparam T
 */
template <typename T>
class ExecutorPoolEpEngineTest : public ExecutorPoolTest<T> {
protected:
    ExecutorPoolEpEngineTest();

    void SetUp() override;
    void TearDown() override;

    // Config string to use when creating EpEngine
    std::string config;
    SynchronousEPEngineUniquePtr engine;
};

class SingleThreadedExecutorPoolTest : public ::testing::Test {
public:
    void SetUp() override {
        {
            NonBucketAllocationGuard guard;
            ExecutorPool::create(ExecutorPool::Backend::Fake);
        }
        pool = ExecutorPool::get();
        pool->registerTaskable(taskable);
    }

    void TearDown() override {
        pool->unregisterTaskable(taskable, false);
        pool->shutdown();
    }

    ExecutorPool* pool;
    ::testing::NiceMock<MockTaskable> taskable;
};

template <typename T>
class ExecutorPoolDynamicWorkerTest : public ExecutorPoolTest<T> {
protected:
    // Simulated number of CPUs. Want a value >16 to be able to test
    // the difference between Default and DiskIOOptimized thread counts.
    const size_t MaxThreads{18};

    void SetUp() override {
        ExecutorPoolTest<T>::SetUp();
        this->makePool(MaxThreads, 2, 2, 2, 2);
        this->pool->registerTaskable(taskable);
    }

    void TearDown() override {
        this->pool->unregisterTaskable(taskable, false);
        this->pool->shutdown();
        ExecutorPoolTest<T>::TearDown();
    }

    /* ThreadGate may still be in use in threads after a test has completed,
     * must be kept around until after the taskable is unregistered and the
     * tasks are certain to have ended, hence its declaration here but
     * assignment in each test case - if it were declared in the scope of
     * the test it would be deleted before the taskable is unregistered.
     */
    std::unique_ptr<ThreadGate> tg;
    ::testing::NiceMock<MockTaskable> taskable;
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
    : public ExecutorPoolTest<TestExecutorPool>,
      public ::testing::WithParamInterface<ThreadCountsParams> {};
