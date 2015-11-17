/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2015 Couchbase, Inc.
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

#include "executor.h"

#include <memory>
#include <mutex>
#include <vector>

/**
 * As the name implies the ExecutorPool is pool of executors to execute
 * tasks. A task is pinned to a thread when it is being scheduled
 * (by using round robin) and never switch the thread.
 */
class ExecutorPool {
public:
    /**
     * Create an executor pool with a given number of worker threads
     */
    ExecutorPool(size_t sz);

    ExecutorPool(const ExecutorPool &) = delete;

    /**
     * Schedule a task for execution at some time. The tasks mutex
     * must be held while calling this method to avoid race conditions.
     *
     * @param task the task to execute
     * @param runnable is the task runnable, or should it be put in
     *                 the wait queue..
     */
    void schedule(std::shared_ptr<Task>& task, bool runnable = true);

private:
    /**
     * The actual list of executors
     */
    std::vector< std::unique_ptr<Executor> > executors;

    /**
     * We'll be using round robin to distribute the tasks to the
     * worker threads
     */
    std::atomic_int roundRobin;
};
