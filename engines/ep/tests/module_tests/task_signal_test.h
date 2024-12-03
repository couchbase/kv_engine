/*
 *     Copyright 2024-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#pragma once

#include "kv_bucket_test.h"
#include "memcached/thread_pool_config.h"

#include <executor/executorpool.h>

class SignalTest;

using namespace std::chrono_literals;

class TaskSignalTest : public KVBucketTest {
public:
    void SetUp() override {
        KVBucketTest::SetUp();
        // Set 3 NonIO threads.
        // All tests will make NonIO tasks.
        originalThreadCount = ExecutorPool::get()->getNumNonIO();
        ExecutorPool::get()->setNumNonIO(ThreadPoolConfig::NonIoThreadCount{3});
    }

    void TearDown() override {
        ExecutorPool::get()->setNumNonIO(
                ThreadPoolConfig::NonIoThreadCount{originalThreadCount});
        KVBucketTest::TearDown();
    }

    std::shared_ptr<SignalTest> makeAndScheduleTask(bool pauseExecution = false,
                                                    bool runAgain = false);
    // Wait for task to be dead, or give up after a timeout
    void waitForTaskCompletion(SignalTest& tasks,
                               std::chrono::seconds timeout = 30s);

private:
    int originalThreadCount;
};