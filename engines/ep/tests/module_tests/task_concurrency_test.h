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
#pragma once

#include "kv_bucket_test.h"

#include <executor/executorpool.h>

#include <platform/semaphore_guard.h>

#include <memory>
#include <thread>
#include <vector>

namespace cb {
class AwaitableSemaphore;
} // namespace cb
class ConcurrencyTestTask;
class ThreadGate;

class TaskConcurrencyTest : public KVBucketTest {
public:
    using Callback = std::function<void(cb::SemaphoreGuard<cb::Semaphore*>&)>;
    void SetUp() override {
        KVBucketTest::SetUp();
        // Set 5 NonIO threads.
        // all tests will make NonIO tasks.
        ExecutorPool::get()->setNumNonIO(5);
    }
    std::vector<std::shared_ptr<ConcurrencyTestTask>> makeTasks(
            cb::AwaitableSemaphore& sem, size_t taskCount, Callback callback);

    void scheduleAll(std::vector<std::shared_ptr<ConcurrencyTestTask>>& tasks);

    // wait for all provided tasks to be dead, or give up after a timeout
    void waitForTasks(std::vector<std::shared_ptr<ConcurrencyTestTask>>& tasks);
};
