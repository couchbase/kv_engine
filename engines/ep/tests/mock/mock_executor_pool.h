/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2019 Couchbase, Inc.
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
#pragma once

#include "executorpool.h"

/*
 * Wrapper to ExecutorPool, exposes protected members for testing.
 */
class MockExecutorPool : public ExecutorPool {
public:
    explicit MockExecutorPool(size_t numTaskSets)
        : ExecutorPool(4 /*maxThreads*/,
                       numTaskSets,
                       ThreadPoolConfig::ThreadCount(1) /*maxReaders*/,
                       ThreadPoolConfig::ThreadCount(1) /*maxWriters*/,
                       1 /*maxAuxIO*/,
                       1 /*maxNonIO*/) {
    }

    /**
     * Replaces the ExecutorPool instance with a MockExecutorPool one.
     * Useful for checking the internal state of ExecutorPool.
     */
    static void replaceExecutorPoolWithMock();

    /**
     * @param queueType
     * @param taskName
     * @return true if the given task (of the given type) is scheduled,
     *     false otherwise
     */
    bool isTaskScheduled(const task_type_t queueType,
                         const std::string& taskName);
};
