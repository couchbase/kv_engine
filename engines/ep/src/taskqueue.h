/*
 *     Copyright 2014-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#pragma once

#include "futurequeue.h"
#include "task_type.h"
#include <platform/syncobject.h>

#include <chrono>
#include <list>
#include <queue>

class CB3ExecutorPool;
class CB3ExecutorThread;

class TaskQueue {
    friend class CB3ExecutorPool;

public:
    TaskQueue(CB3ExecutorPool* m, task_type_t t, const char* nm);
    ~TaskQueue();

    void schedule(ExTask &task);

    /**
     * Reschedules the given task, adding it onto the futureQueue (sorted by
     * each task's waketime.
     *
     * @param task Task to reschedule.
     * @return The waketime of the earliest (next) task in the futureQueue -
     *         note this isn't necessarily the same as `task`.
     */
    std::chrono::steady_clock::time_point reschedule(ExTask& task);

    void doWake(size_t &numToWake);

    /**
     * Fetch the next task to be run from the task queues, updating
     * thread::currentTask with the next task to run (if one found).
     * @returns true if there is a task to run, otherwise false.
     */
    bool fetchNextTask(CB3ExecutorThread& thread);

    /**
     * Sleeps until the next task is ready to run, waking up when ready and
     * updating thread::currentTask with the task to run.
     * @returns true if there is a task to run, otherwise false.
     */
    bool sleepThenFetchNextTask(CB3ExecutorThread& thread);

    void wake(ExTask &task);

    const std::string getName() const;

    task_type_t getQueueType() const {
        return queueType;
    }

    size_t getReadyQueueSize();

    size_t getFutureQueueSize();

    void snooze(ExTask& task, const double secs) {
        futureQueue.snooze(task, secs);
    }

private:
    void _schedule(ExTask &task);
    std::chrono::steady_clock::time_point _reschedule(ExTask& task);
    bool _sleepThenFetchNextTask(CB3ExecutorThread& t);
    bool _fetchNextTask(CB3ExecutorThread& thread);
    bool _fetchNextTaskInner(CB3ExecutorThread& t,
                             const std::unique_lock<std::mutex>& lh);
    void _wake(ExTask &task);
    bool _doSleep(CB3ExecutorThread& thread,
                  std::unique_lock<std::mutex>& lock);
    void _doWake_UNLOCKED(size_t &numToWake);
    size_t _moveReadyTasks(const std::chrono::steady_clock::time_point tv);
    ExTask _popReadyTask();

    SyncObject mutex;
    const std::string name;
    task_type_t queueType;
    CB3ExecutorPool* manager;
    size_t sleepers; // number of threads sleeping in this taskQueue

    // sorted by task priority.
    std::priority_queue<ExTask, std::deque<ExTask>,
                        CompareByPriority> readyQueue;

    // sorted by waketime. Guarded by `mutex`.
    FutureQueue<> futureQueue;
};
