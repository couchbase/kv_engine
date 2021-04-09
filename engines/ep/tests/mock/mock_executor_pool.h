/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2019-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#pragma once

#include "cb3_executorpool.h"
#include "task_type.h"

/*
 * Wrapper to ExecutorPool, exposes protected members for testing.
 */
class MockExecutorPool : public CB3ExecutorPool {
public:
    explicit MockExecutorPool()
        : CB3ExecutorPool(4 /*maxThreads*/,
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
