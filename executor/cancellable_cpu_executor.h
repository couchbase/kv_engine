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

#include "globaltask.h"

#include <folly/concurrency/UnboundedQueue.h>
#include <folly/executors/CPUThreadPoolExecutor.h>

/**
 * Custom folly CPU thread pool executor which puts work in a separate queue
 * that we control so that we can remove tasks as and when a bucket goes away
 * without having to actually run them (seen to noticeably slow down bucket
 * deinitialization which can case rebalance failures).
 */
class CancellableCPUExecutor {
public:
    CancellableCPUExecutor(size_t numThreads,
                           std::shared_ptr<folly::ThreadFactory> threadFactory)
        : cpuPool(numThreads, std::move(threadFactory)) {
    }

    size_t numThreads() const {
        return cpuPool.numThreads();
    }

    void setNumThreads(size_t numThreads) {
        cpuPool.setNumThreads(numThreads);
    }

    folly::ThreadPoolExecutor::PoolStats getPoolStats() const {
        return cpuPool.getPoolStats();
    }

    size_t getPendingTaskCount() const {
        return cpuPool.getPendingTaskCount();
    }

    size_t getTaskQueueSize() const {
        return cpuPool.getTaskQueueSize();
    }

    void join() {
        cpuPool.join();
    }

    /**
     * Add a task to run
     *
     * @param task which includes the taskable and taskId that we need in this
     *        class
     * @param func the function to run (which runs the task)
     */
    void addWithTask(GlobalTask& task, folly::Func func);

    /**
     * Remove tasks for the given taskable from our work queue. Should always
     * be run on the SchedulerPool (EventBase) to ensure that tasks are not
     * scheduled on this pool while we run.
     *
     * @param taskable to remove tasks for
     * @return vector of GlobalTask* for this taskable
     */
    std::vector<GlobalTask*> removeTasksForTaskable(const Taskable& taskable);

private:
    /**
     * Element of our queue
     */
    struct QueueElem {
        QueueElem(GlobalTask& task, folly::Func func)
            : task(task), func(std::move(func)) {
        }

        // Taking a GlobalTask& here because it contains both the Taskable
        // reference and the taskId which we need to cancel tasks. We take a
        // GlobalTask& over an ExTask to avoid shared_ptr promotions for the
        // sake of efficiency.
        GlobalTask& task;
        folly::Func func;
    };

    /**
     * Queue of tasks to run. Uses the same queue type as
     * folly::CPUThreadPoolExecutor. The task here contians the actual work we
     * want to run.
     */
    folly::UMPMCQueue<QueueElem, false, 6> tasks;

    /**
     * Underlying CPUThreadPoolExecutor which runs "notification" tasks to run a
     * task in our tasks queue.
     */
    folly::CPUThreadPoolExecutor cpuPool;
};
