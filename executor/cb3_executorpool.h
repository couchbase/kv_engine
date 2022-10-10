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

/*
 * === High-level overview of the task execution system. ===
 *
 * ExecutorPool is the core interface for users wishing to run tasks on our
 * worker threads.
 *
 * Under the covers we have a configurable number of system threads that are
 * labeled with a type (see task_type_t). These threads service all buckets.
 *
 * Each thread operates by reading from a shared TaskQueue. Each thread wakes
 * up and fetches (TaskQueue::fetchNextTask) a task for execution
 * (GlobalTask::run() is called to execute the task).
 *
 * The pool also has the concept of high and low priority which is achieved by
 * having two TaskQueue objects per task-type. When a thread wakes up to run
 * a task, it will service the high-priority queue more frequently than the
 * low-priority queue.
 *
 * Within a single queue itself there is also a task priority. The task priority
 * is a value where lower is better. When many tasks are ready for execution
 * they are moved to a ready queue and sorted by their priority. Thus tasks
 * with priority 0 get to go before tasks with priority 1. Only once the ready
 * queue of tasks is empty will we consider looking for more eligible tasks.
 * In this context, an eligible task is one that has a wakeTime <= now.
 *
 * === Important methods of the ExecutorPool ===
 *
 * ExecutorPool* ExecutorPool::get()
 *   The ExecutorPool is accessed via the static get() method. Calling get
 *   returns the processes global ExecutorPool object. This is an instance
 *   that is global/shared between all buckets.
 *
 * ExecutorPool::schedule(ExTask task, task_type_t qidx)
 *   The schedule method allows task to be scheduled for future execution by a
 *   thread of type 'qidx'. The task's 'wakeTime' determines approximately when
 *   the task will be executed (no guarantees).
 *
 * ExecutorPool::wake(size_t taskId)
 *   The wake method allows for a caller to request that the task matching
 *   taskId be executed by its thread-type now'. The tasks wakeTime is modified
 *   so that it has a wakeTime of now and a thread of the correct type is
 *   signaled to wake-up and perform fetching. The woken task will have to wait
 *   for any current tasks to be executed first, but it will jump ahead of other
 *   tasks as tasks that are ready to run are ordered by their priority.
 *
 * ExecutorPool::snooze(size_t taskId, double toSleep)
 *   The pool's snooze method will locate the task matching taskId and adjust
 *   its wakeTime to account for the toSleep value.
 */

#include "executorpool.h"
#include "task_type.h"
#include "taskable.h"
#include <memcached/thread_pool_config.h>
#include <platform/syncobject.h>
#include <map>
#include <set>
#include <vector>

// Forward decl
class CookieIface;
class TaskQueue;
class CB3ExecutorThread;

using ThreadQ = std::vector<std::unique_ptr<CB3ExecutorThread>>;
using TaskQpair = std::pair<ExTask, TaskQueue*>;
using TaskQ = std::vector<TaskQueue*>;

class CB3ExecutorPool : public ExecutorPool {
public:
    /**
     * Construct an ExecutorPool.
     *
     * @param maxThreads Maximum number of threads in any given thread class
     *                   (Reader, Writer, NonIO, AuxIO). A value of 0 means
     *                   use number of CPU cores.
     * @param maxReaders Number of Reader threads to create.
     * @param maxWriters Number of Writer threads to create.
     * @param maxAuxIO Number of AuxIO threads to create (0 = auto-configure).
     * @param maxNonIO Number of NonIO threads to create (0 = auto-configure).
     */
    CB3ExecutorPool(size_t maxThreads,
                    ThreadPoolConfig::ThreadCount maxReaders,
                    ThreadPoolConfig::ThreadCount maxWriters,
                    size_t maxAuxIO,
                    size_t maxNonIO);

    ~CB3ExecutorPool() override;

    void addWork(size_t newWork, task_type_t qType);

    void lessWork(task_type_t qType);

    void startWork(task_type_t taskType);

    void doneWork(task_type_t taskType);

    bool trySleep(task_type_t task_type) {
        if (!numReadyTasks[task_type]) {
            numSleepers++;
            return true;
        }
        return false;
    }

    void woke() {
        numSleepers--;
    }

    TaskQueue* nextTask(CB3ExecutorThread& t, uint8_t tick);

    TaskQueue* getSleepQ(unsigned int curTaskType) {
        return isHiPrioQset ? hpTaskQ[curTaskType] : lpTaskQ[curTaskType];
    }

    bool cancel(size_t taskId, bool remove = false) override;

    bool wakeAndWait(size_t taskId) override;

    /**
     * Change how many worker threads there are for a given task type,
     * stopping/starting threads to reach the desired number.
     *
     * @param type the type of task for which to adjust the workers
     * @param newCount Target number of worker threads
     */
    void adjustWorkers(task_type_t type, size_t newCount);

    bool snoozeAndWait(size_t taskId, double tosleep) override;

    void registerTaskable(Taskable& taskable) override;

    void unregisterTaskable(Taskable& taskable, bool force) override;

    size_t getNumTaskables() const override {
        return numTaskables;
    }

    void doWorkerStat(Taskable& taskable,
                      const CookieIface& cookie,
                      const AddStatFn& add_stat) override;

    /**
     * Generates stats regarding currently running tasks, as displayed by
     * cbstats tasks.
     */
    void doTasksStat(Taskable& taskable,
                     const CookieIface& cookie,
                     const AddStatFn& add_stat) override;

    void doTaskQStat(Taskable& taskable,
                     const CookieIface& cookie,
                     const AddStatFn& add_stat) override;

    size_t getNumWorkersStat() override {
        std::lock_guard<std::mutex> lh(tMutex);
        return threadQ.size();
    }

    size_t getNumReaders() override;

    size_t getNumWriters() override;

    size_t getNumAuxIO() override;

    size_t getNumNonIO() override;

    void setNumReaders(ThreadPoolConfig::ThreadCount v) override {
        adjustWorkers(READER_TASK_IDX, calcNumReaders(v));
    }

    void setNumWriters(ThreadPoolConfig::ThreadCount v) override {
        adjustWorkers(WRITER_TASK_IDX, calcNumWriters(v));
    }

    void setNumAuxIO(uint16_t v) override {
        adjustWorkers(AUXIO_TASK_IDX, v);
    }

    void setNumNonIO(uint16_t v) override {
        adjustWorkers(NONIO_TASK_IDX, v);
    }

    size_t getNumReadyTasks() override {
        return totReadyTasks;
    }

    size_t getNumSleepers() override {
        return numSleepers;
    }

    size_t schedule(ExTask task) override;

protected:
    TaskQueue* _nextTask(CB3ExecutorThread& t, uint8_t tick);

    /**
     * see cancel() for detail
     *
     * @param taskId Task to cancel
     * @param remove true if the task should be removed from taskLocator
     * @return if the task is located it is returned
     */
    ExTask _cancel(size_t taskId, bool remove = false);

    bool _wake(size_t taskId);
    virtual bool _startWorkers();

    /**
     * Change the number of worked threads.
     *
     * @param type Thread type to change
     * @param desiredNumItems Number of threads we want to result in.
     */
    void _adjustWorkers(task_type_t type, size_t desiredNumItems);

    bool _snooze(size_t taskId, double tosleep);
    size_t _schedule(ExTask task);
    void _registerTaskable(Taskable& taskable);
    void _unregisterTaskable(Taskable& taskable, bool force);
    std::vector<ExTask> _stopTaskGroup(task_gid_t taskGID,
                                       std::unique_lock<std::mutex>& lh,
                                       bool force);
    TaskQueue* _getTaskQueue(const Taskable& t, task_type_t qidx);
    void _stopAndJoinThreads();

    const size_t numTaskSets{NUM_TASK_GROUPS};

    std::atomic<size_t> totReadyTasks;
    SyncObject mutex; // Thread management condition var + mutex

    //! A mapping of task ids to Task, TaskQ in the thread pool
    std::map<size_t, TaskQpair> taskLocator;

    // A list of threads
    ThreadQ threadQ;

    // Global cross bucket priority queues where tasks get scheduled into ...
    TaskQ hpTaskQ; // a vector array of numTaskSets elements for high priority
    std::atomic_bool isHiPrioQset;

    TaskQ lpTaskQ; // a vector array of numTaskSets elements for low priority
    std::atomic_bool isLowPrioQset;

    // Numbers of registered taskables.
    size_t numTaskables;

    // to serialize taskLocator, threadQ, numBuckets access
    mutable SyncObject tMutex;

    std::atomic<uint16_t> numSleepers; // total number of sleeping threads
    std::vector<std::atomic<uint16_t>>
            curWorkers; // track # of active workers per TaskSet
    // and limit it to the value set here
    std::vector<std::atomic<int>> numWorkers;
    std::vector<std::atomic<size_t>>
            numReadyTasks; // number of ready tasks per task set

    // Set of all known task owners
    std::set<void*> taskOwners;

    /// To allow ExecutorPool::get() to create an instance.
    friend class ExecutorPool;
};
