/*
 *     Copyright 2014 Couchbase, Inc.
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

#include "futurequeue.h"
#include "syncobject.h"
#include "task_type.h"

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

    static const std::string taskType2Str(task_type_t type);

    const std::string getName() const;

    const task_type_t getQueueType() const { return queueType; }

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
