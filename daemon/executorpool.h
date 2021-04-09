/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2015-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#pragma once

#include <atomic>
#include <memory>
#include <mutex>
#include <vector>

namespace cb {
struct ProcessClockSource;
}
class Executor;
class Task;

namespace cb {

/**
 * As the name implies the ExecutorPool is pool of executors to execute
 * tasks. A task is pinned to a thread when it is being scheduled
 * (by using round robin) and never switch the thread.
 */
class ExecutorPool {
public:
    /**
     * Create an executor pool with a given number of worker threads
     * and the default clock source.
     */
    explicit ExecutorPool(size_t sz);

    /**
     * Create an executor pool with a given number of worker threads
     * and an injected clock source
     */
    ExecutorPool(size_t sz, cb::ProcessClockSource& clock);

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

    /**
     * This method should be called periodically (e.g. once a second)
     * to make tasks that are scheduled to run the future runnable.
     */
    void clockTick();

    // @todo expose these as stats

    size_t waitqSize() const;

    size_t runqSize() const;

    size_t futureqSize() const;

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

} // namespace cb
