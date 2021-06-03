/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2020-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "cb3_executorpool.h"
#include "cb3_executorthread.h"
#include "taskqueue.h"

#include <engines/ep/src/bucket_logger.h>
#include <engines/ep/src/ep_engine.h>
#include <engines/ep/src/ep_time.h>
#include <engines/ep/src/objectregistry.h>
#include <nlohmann/json.hpp>
#include <platform/checked_snprintf.h>
#include <platform/string_hex.h>
#include <platform/sysinfo.h>
#include <statistics/cbstat_collector.h>
#include <algorithm>
#include <chrono>
#include <sstream>

size_t CB3ExecutorPool::getNumNonIO() {
    return calcNumNonIO(numWorkers[NONIO_TASK_IDX].load());
}

size_t CB3ExecutorPool::getNumAuxIO() {
    return calcNumAuxIO(numWorkers[AUXIO_TASK_IDX].load());
}

size_t CB3ExecutorPool::getNumWriters() {
    return calcNumWriters(
            ThreadPoolConfig::ThreadCount(numWorkers[WRITER_TASK_IDX].load()));
}

size_t CB3ExecutorPool::getNumReaders() {
    return calcNumReaders(
            ThreadPoolConfig::ThreadCount(numWorkers[READER_TASK_IDX].load()));
}

CB3ExecutorPool::CB3ExecutorPool(size_t maxThreads,
                                 ThreadPoolConfig::ThreadCount maxReaders,
                                 ThreadPoolConfig::ThreadCount maxWriters,
                                 size_t maxAuxIO,
                                 size_t maxNonIO)
    : ExecutorPool(maxThreads),
      totReadyTasks(0),
      isHiPrioQset(false),
      isLowPrioQset(false),
      numTaskables(0),
      numSleepers(0),
      curWorkers(numTaskSets),
      numWorkers(numTaskSets),
      numReadyTasks(numTaskSets) {
    for (size_t i = 0; i < numTaskSets; i++) {
        curWorkers[i] = 0;
        numReadyTasks[i] = 0;
    }
    numWorkers[WRITER_TASK_IDX] = static_cast<int>(maxWriters);
    numWorkers[READER_TASK_IDX] = static_cast<int>(maxReaders);
    numWorkers[AUXIO_TASK_IDX] = maxAuxIO;
    numWorkers[NONIO_TASK_IDX] = maxNonIO;
}

CB3ExecutorPool::~CB3ExecutorPool() {
    _stopAndJoinThreads();

    if (isHiPrioQset) {
        for (size_t i = 0; i < numTaskSets; i++) {
            delete hpTaskQ[i];
        }
    }
    if (isLowPrioQset) {
        for (size_t i = 0; i < numTaskSets; i++) {
            delete lpTaskQ[i];
        }
    }
}

// To prevent starvation of low priority queues, we define their
// polling frequencies as follows ...
#define LOW_PRIORITY_FREQ 5 // 1 out of 5 times threads check low priority Q

TaskQueue* CB3ExecutorPool::_nextTask(CB3ExecutorThread& t, uint8_t tick) {
    if (!tick) {
        return nullptr;
    }

    task_type_t myq = t.taskType;
    TaskQueue* checkQ; // which TaskQueue set should be polled first
    TaskQueue* checkNextQ; // which set of TaskQueue should be polled next
    TaskQueue* toggle = nullptr;
    if (!(tick % LOW_PRIORITY_FREQ)) { // if only 1 Q set, both point to it
        checkQ = isLowPrioQset ? lpTaskQ[myq]
                               : (isHiPrioQset ? hpTaskQ[myq] : nullptr);
        checkNextQ = isHiPrioQset ? hpTaskQ[myq] : checkQ;
    } else {
        checkQ = isHiPrioQset ? hpTaskQ[myq]
                              : (isLowPrioQset ? lpTaskQ[myq] : nullptr);
        checkNextQ = isLowPrioQset ? lpTaskQ[myq] : checkQ;
    }
    while (t.state == EXECUTOR_RUNNING) {
        if (checkQ && checkQ->fetchNextTask(t)) {
            return checkQ;
        }
        if (toggle || checkQ == checkNextQ) {
            TaskQueue* sleepQ = getSleepQ(myq);
            if (sleepQ->sleepThenFetchNextTask(t)) {
                return sleepQ;
            } else {
                return nullptr;
            }
        }
        toggle = checkQ;
        checkQ = checkNextQ;
        checkNextQ = toggle;
    }
    return nullptr;
}

TaskQueue* CB3ExecutorPool::nextTask(CB3ExecutorThread& t, uint8_t tick) {
    TaskQueue* tq = _nextTask(t, tick);
    return tq;
}

void CB3ExecutorPool::addWork(size_t newWork, task_type_t qType) {
    if (newWork) {
        totReadyTasks.fetch_add(newWork);
        numReadyTasks[qType].fetch_add(newWork);
    }
}

void CB3ExecutorPool::lessWork(task_type_t qType) {
    if (numReadyTasks[qType].load() == 0) {
        throw std::logic_error(
                "CB3ExecutorPool::lessWork: number of ready "
                "tasks on qType " +
                std::to_string(qType) + " is zero");
    }
    numReadyTasks[qType]--;
    totReadyTasks--;
}

void CB3ExecutorPool::startWork(task_type_t taskType) {
    if (taskType == NO_TASK_TYPE || taskType == NUM_TASK_GROUPS) {
        throw std::logic_error(
                "CB3ExecutorPool::startWork: worker is starting task with "
                "invalid "
                "type {" +
                std::to_string(taskType) + "}");
    } else {
        ++curWorkers[taskType];
        EP_LOG_TRACE(
                "Taking up work in task "
                "type:{{{}}} "
                "current:{{{}}}, max:{{{}}}",
                taskType,
                curWorkers[taskType].load(),
                numWorkers[taskType].load());
    }
}

void CB3ExecutorPool::doneWork(task_type_t taskType) {
    if (taskType == NO_TASK_TYPE || taskType == NUM_TASK_GROUPS) {
        throw std::logic_error(
                "CB3ExecutorPool::doneWork: worker is finishing task with "
                "invalid "
                "type {" +
                std::to_string(taskType) + "}");
    } else {
        --curWorkers[taskType];
        // Record that a thread is done working on a particular queue type
        EP_LOG_TRACE("Done with task type:{{{}}} capacity:{{{}}}",
                     taskType,
                     numWorkers[taskType].load());
    }
}

ExTask CB3ExecutorPool::_cancel(size_t taskId, bool remove) {
    std::lock_guard<std::mutex> lh(tMutex);
    auto itr = taskLocator.find(taskId);
    if (itr == taskLocator.end()) {
        EP_LOG_DEBUG("Task id {} not found", uint64_t(taskId));
        return {};
    }

    ExTask task = itr->second.first;
    EP_LOG_TRACE("Cancel task {} id {} on bucket {} {}",
                 task->getDescription(),
                 task->getId(),
                 task->getTaskable().getName(),
                 remove ? "removed task" : "");

    task->cancel(); // must be idempotent, just set state to dead

    if (remove) { // only internal threads can remove tasks
        if (!task->isdead()) {
            throw std::logic_error("CB3ExecutorPool::_cancel: task '" +
                                   task->getDescription() +
                                   "' is not dead after calling "
                                   "cancel() on it");
        }
        taskLocator.erase(itr);
        tMutex.notify_all();
    } else { // wake up the task from the TaskQ so a thread can safely erase it
             // otherwise we may race with unregisterTaskable where a unlocated
             // task runs in spite of its bucket getting unregistered
        itr->second.second->wake(task);
    }
    return task;
}

bool CB3ExecutorPool::cancel(size_t taskId, bool remove) {
    ExTask task;

    // Memory allocation guards.
    // cancel is called from a number of places and the caller may or may not
    // have memory allocation tracking associated to a bucket. For example
    // CB3ExecutorThread::run will not be associated with a bucket when it calls
    // cancel, yet KVBucket::setExpiryPagerSleeptime will be associated with a
    // bucket.
    // The call to _cancel *must* not be associated with a bucket, any memory
    // alloc/dealloc (dealloc can occur as we call erase on a container) is not
    // accounted to a bucket.
    // If a task is being removed, its deallocation must be associated to the
    // bucket, so an explicit BucketAllocationGuard encloses the forced reset
    // of 'task'
    {
        NonBucketAllocationGuard guard;
        task = _cancel(taskId, remove);
    }

    // It is critical that the task is 'reset' without holding
    // ExecutorPool::tMutex. _cancel is the part of 'cancel' which obtains the
    // mutex. The mutex must not be held because if the task referenced
    // from the shared_ptr does destruct, the destructor is allowed to call
    // other ExecutorPool methods, e.g. pool->schedule(my_cleanup_task);
    bool taskFound = task != nullptr;
    if (task) {
        BucketAllocationGuard bucketGuard(task->getEngine());
        task.reset();
    }
    return taskFound;
}

bool CB3ExecutorPool::_wake(size_t taskId) {
    std::lock_guard<std::mutex> lh(tMutex);
    auto itr = taskLocator.find(taskId);
    if (itr != taskLocator.end()) {
        itr->second.second->wake(itr->second.first);
        return true;
    }
    return false;
}

bool CB3ExecutorPool::wakeAndWait(size_t taskId) {
    NonBucketAllocationGuard guard;
    bool rv = _wake(taskId);
    return rv;
}

bool CB3ExecutorPool::_snooze(size_t taskId, double toSleep) {
    std::lock_guard<std::mutex> lh(tMutex);
    auto itr = taskLocator.find(taskId);
    if (itr != taskLocator.end()) {
        itr->second.second->snooze(itr->second.first, toSleep);
        return true;
    }
    return false;
}

bool CB3ExecutorPool::snoozeAndWait(size_t taskId, double toSleep) {
    NonBucketAllocationGuard guard;
    bool rv = _snooze(taskId, toSleep);
    return rv;
}

TaskQueue* CB3ExecutorPool::_getTaskQueue(const Taskable& t, task_type_t qidx) {
    TaskQueue* q = nullptr;
    size_t curNumThreads = 0;

    bucket_priority_t bucketPriority = t.getWorkloadPriority();

    if (qidx < 0 || static_cast<size_t>(qidx) >= numTaskSets) {
        throw std::invalid_argument(
                "CB3ExecutorPool::_getTaskQueue: qidx "
                "(which is " +
                std::to_string(qidx) + ") is outside the range [0," +
                std::to_string(numTaskSets) + ")");
    }

    curNumThreads = threadQ.size();

    if (!bucketPriority) {
        EP_LOG_WARN(
                "Trying to schedule task for unregistered "
                "bucket {}",
                t.getName());
        return q;
    }

    if (curNumThreads < maxGlobalThreads) {
        if (isHiPrioQset) {
            q = hpTaskQ[qidx];
        } else if (isLowPrioQset) {
            q = lpTaskQ[qidx];
        }
    } else { // Max capacity Mode scheduling ...
        switch (bucketPriority) {
        case LOW_BUCKET_PRIORITY:
            if (lpTaskQ.size() != numTaskSets) {
                throw std::logic_error(
                        "CB3ExecutorPool::_getTaskQueue: At "
                        "maximum capacity but low-priority taskQ size "
                        "(which is " +
                        std::to_string(lpTaskQ.size()) + ") is not " +
                        std::to_string(numTaskSets));
            }
            q = lpTaskQ[qidx];
            break;

        case HIGH_BUCKET_PRIORITY:
            if (hpTaskQ.size() != numTaskSets) {
                throw std::logic_error(
                        "CB3ExecutorPool::_getTaskQueue: At "
                        "maximum capacity but high-priority taskQ size "
                        "(which is " +
                        std::to_string(lpTaskQ.size()) + ") is not " +
                        std::to_string(numTaskSets));
            }
            q = hpTaskQ[qidx];
            break;

        default:
            throw std::logic_error(
                    "CB3ExecutorPool::_getTaskQueue: Invalid "
                    "bucketPriority " +
                    std::to_string(bucketPriority));
        }
    }
    return q;
}

size_t CB3ExecutorPool::_schedule(ExTask task) {
    std::lock_guard<std::mutex> lh(tMutex);
    const size_t taskId = task->getId();

    TaskQueue* q = _getTaskQueue(task->getTaskable(),
                                 GlobalTask::getTaskType(task->getTaskId()));
    TaskQpair tqp(task, q);

    auto result = taskLocator.insert(std::make_pair(taskId, tqp));

    if (result.second) {
        // tqp was inserted; it was not already present. Prevents multiple
        // copies of a task being present in the task queues.
        q->schedule(task);
    }

    return taskId;
}

size_t CB3ExecutorPool::schedule(ExTask task) {
    NonBucketAllocationGuard guard;
    size_t rv = _schedule(task);
    return rv;
}

void CB3ExecutorPool::_registerTaskable(Taskable& taskable) {
    TaskQ* taskQ;
    std::atomic_bool* whichQset;
    const char* queueName;
    WorkLoadPolicy& workload = taskable.getWorkLoadPolicy();
    bucket_priority_t priority = workload.getBucketPriority();

    if (priority < HIGH_BUCKET_PRIORITY) {
        taskable.setWorkloadPriority(LOW_BUCKET_PRIORITY);
        taskQ = &lpTaskQ;
        whichQset = &isLowPrioQset;
        queueName = "LowPrioQ_";
        EP_LOG_INFO("Taskable {} registered with low priority",
                    taskable.getName());
    } else {
        taskable.setWorkloadPriority(HIGH_BUCKET_PRIORITY);
        taskQ = &hpTaskQ;
        whichQset = &isHiPrioQset;
        queueName = "HiPrioQ_";
        EP_LOG_INFO("Taskable {} registered with high priority",
                    taskable.getName());
    }

    {
        std::lock_guard<std::mutex> lh(tMutex);

        if (!(*whichQset)) {
            taskQ->reserve(numTaskSets);
            for (size_t i = 0; i < numTaskSets; ++i) {
                taskQ->push_back(
                        new TaskQueue(this, (task_type_t)i, queueName));
            }
            *whichQset = true;
        }

        taskOwners.insert(&taskable);
        numTaskables++;
    }

    _startWorkers();
}

void CB3ExecutorPool::registerTaskable(Taskable& taskable) {
    NonBucketAllocationGuard guard;
    _registerTaskable(taskable);
}

void CB3ExecutorPool::_adjustWorkers(task_type_t type, size_t desiredNumItems) {
    std::string typeName{to_string(type)};

    // vector of threads which have been stopped
    // and should be joined after unlocking, if any.
    ThreadQ removed;

    size_t numItems;

    {
        // Lock mutex, we are modifying threadQ
        std::lock_guard<std::mutex> lh(tMutex);

        // How many threads performing this task type there are currently
        numItems = std::count_if(
                threadQ.begin(),
                threadQ.end(),
                [type](std::unique_ptr<CB3ExecutorThread>& thread) {
                    return thread->taskType == type;
                });

        if (numItems == desiredNumItems) {
            return;
        }

        EP_LOG_INFO("Adjusting threads of type:{} from:{} to:{}",
                    typeName,
                    numItems,
                    desiredNumItems);

        if (numItems < desiredNumItems) {
            // If we want to increase the number of threads, they must be
            // created and started
            for (size_t tidx = numItems; tidx < desiredNumItems; ++tidx) {
                threadQ.push_back(std::make_unique<CB3ExecutorThread>(
                        this,
                        type,
                        typeName + "_worker_" + std::to_string(tidx)));
                threadQ.back()->start();
            }
        } else if (numItems > desiredNumItems) {
            // If we want to decrease the number of threads, they must be
            // identified in the threadQ, stopped, and removed.
            size_t toRemove = numItems - desiredNumItems;

            auto itr = threadQ.rbegin();
            while (itr != threadQ.rend() && toRemove) {
                if ((*itr)->taskType == type) {
                    // stop but /don't/ join yet
                    (*itr)->stop(false);

                    // store temporarily
                    removed.push_back(std::move(*itr));

                    // remove from the threadQ
                    itr = ThreadQ::reverse_iterator(
                            threadQ.erase(std::next(itr).base()));
                    --toRemove;
                } else {
                    ++itr;
                }
            }
        }

        numWorkers[type] = desiredNumItems;
    } // release mutex

    // MB-22938 wake all threads to avoid blocking if a thread is sleeping
    // waiting for work. Without this, stopping a single thread could take
    // up to 2s (MIN_SLEEP_TIME).
    if (!removed.empty()) {
        TaskQueue* sleepQ = getSleepQ(type);
        size_t threadCount = threadQ.size();
        sleepQ->doWake(threadCount);
    }

    // We could not join the threads while holding the lock, as some operations
    // called from the threads (such as schedule) acquire the lock - we could
    // have caused deadlock by waiting for the thread to complete its task and
    // exit, while it waits to acquire the lock.
    auto itr = removed.begin();
    while (itr != removed.end()) {
        (*itr)->stop(true);
        itr = removed.erase(itr);
    }
}

void CB3ExecutorPool::adjustWorkers(task_type_t type, size_t newCount) {
    NonBucketAllocationGuard guard;
    _adjustWorkers(type, newCount);
}

bool CB3ExecutorPool::_startWorkers() {
    size_t numReaders = getNumReaders();
    size_t numWriters = getNumWriters();
    size_t numAuxIO = getNumAuxIO();
    size_t numNonIO = getNumNonIO();

    _adjustWorkers(READER_TASK_IDX, numReaders);
    _adjustWorkers(WRITER_TASK_IDX, numWriters);
    _adjustWorkers(AUXIO_TASK_IDX, numAuxIO);
    _adjustWorkers(NONIO_TASK_IDX, numNonIO);

    return true;
}

std::vector<ExTask> CB3ExecutorPool::_stopTaskGroup(
        task_gid_t taskGID, std::unique_lock<std::mutex>& lh, bool force) {
    bool unfinishedTask;
    std::map<size_t, TaskQpair>::iterator itr;

    std::vector<ExTask> tasks;
    // Gather the tasks which match the taskGID
    for (itr = taskLocator.begin(); itr != taskLocator.end(); itr++) {
        if (itr->second.first->getTaskable().getGID() == taskGID) {
            tasks.push_back(itr->second.first);
        }
    }

    do {
        unfinishedTask = false;
        for (itr = taskLocator.begin(); itr != taskLocator.end(); itr++) {
            ExTask& task = itr->second.first;
            TaskQueue* q = itr->second.second;
            if (task->getTaskable().getGID() == taskGID) {
                EP_LOG_INFO("Stopping Task id {} {} {}",
                            uint64_t(task->getId()),
                            task->getTaskable().getName(),
                            task->getDescription());
                // If force flag is set during shutdown, cancel all tasks
                // without considering the blockShutdown status of the task.
                if (force || !task->blockShutdown) {
                    task->cancel(); // Must be idempotent
                }
                q->wake(task);
                unfinishedTask = true;
            }
        }
        if (unfinishedTask) {
            // Wait till task gets cancelled
            tMutex.wait_for(lh, MIN_SLEEP_TIME);
        }
    } while (unfinishedTask);

    return tasks;
}

std::vector<ExTask> CB3ExecutorPool::_unregisterTaskable(Taskable& taskable,
                                                         bool force) {
    EP_LOG_INFO("Unregistering {} taskable {}",
                (numTaskables == 1) ? "last" : "",
                taskable.getName());

    std::unique_lock<std::mutex> lh(tMutex);

    auto rv = _stopTaskGroup(taskable.getGID(), lh, force);

    taskOwners.erase(&taskable);
    if (!(--numTaskables)) {
        if (!taskLocator.empty()) {
            throw std::logic_error(
                    "CB3ExecutorPool::_unregisterTaskable: "
                    "Attempting to unregister taskable '" +
                    taskable.getName() + "' but taskLocator is not empty");
        }
        for (auto& tidx : threadQ) {
            tidx->stop(false); // only set state to DEAD
        }

        for (unsigned int idx = 0; idx < numTaskSets; idx++) {
            TaskQueue* sleepQ = getSleepQ(idx);
            size_t wakeAll = threadQ.size();
            sleepQ->doWake(wakeAll);
        }

        for (auto& tidx : threadQ) {
            tidx->stop(/*wait for threads */);
            tidx.reset();
        }

        for (size_t i = 0; i < numTaskSets; i++) {
            curWorkers[i] = 0;
        }

        threadQ.clear();
        if (isHiPrioQset) {
            for (size_t i = 0; i < numTaskSets; i++) {
                delete hpTaskQ[i];
            }
            hpTaskQ.clear();
            isHiPrioQset = false;
        }
        if (isLowPrioQset) {
            for (size_t i = 0; i < numTaskSets; i++) {
                delete lpTaskQ[i];
            }
            lpTaskQ.clear();
            isLowPrioQset = false;
        }
    }
    return rv;
}

std::vector<ExTask> CB3ExecutorPool::unregisterTaskable(Taskable& taskable,
                                                        bool force) {
    // Note: unregistering a bucket is special - any memory allocations /
    // deallocations made while unregistering *should* be accounted to the
    // bucket in question - hence no `onSwitchThread(NULL)` call.
    return _unregisterTaskable(taskable, force);
}

void CB3ExecutorPool::doTaskQStat(Taskable& taskable,
                                  const CookieIface* cookie,
                                  const AddStatFn& add_stat) {
    if (taskable.isShutdown()) {
        return;
    }

    NonBucketAllocationGuard guard;
    try {
        std::array<char, 80> statname{};
        if (isHiPrioQset) {
            for (size_t i = 0; i < numTaskSets; i++) {
                checked_snprintf(statname.data(),
                                 statname.size(),
                                 "ep_workload:%s:InQsize",
                                 hpTaskQ[i]->getName().c_str());
                add_casted_stat(statname.data(),
                                hpTaskQ[i]->getFutureQueueSize(),
                                add_stat,
                                cookie);
                checked_snprintf(statname.data(),
                                 statname.size(),
                                 "ep_workload:%s:OutQsize",
                                 hpTaskQ[i]->getName().c_str());
                add_casted_stat(statname.data(),
                                hpTaskQ[i]->getReadyQueueSize(),
                                add_stat,
                                cookie);
            }
        }
        if (isLowPrioQset) {
            for (size_t i = 0; i < numTaskSets; i++) {
                checked_snprintf(statname.data(),
                                 (statname.size()),
                                 "ep_workload:%s:InQsize",
                                 lpTaskQ[i]->getName().c_str());
                add_casted_stat(statname.data(),
                                lpTaskQ[i]->getFutureQueueSize(),
                                add_stat,
                                cookie);
                checked_snprintf(statname.data(),
                                 statname.size(),
                                 "ep_workload:%s:OutQsize",
                                 lpTaskQ[i]->getName().c_str());
                add_casted_stat(statname.data(),
                                lpTaskQ[i]->getReadyQueueSize(),
                                add_stat,
                                cookie);
            }
        }
    } catch (std::exception& error) {
        EP_LOG_WARN("CB3ExecutorPool::doTaskQStat: Failed to build stats: {}",
                    error.what());
    }
}

static void addWorkerStats(const char* prefix,
                           CB3ExecutorThread& t,
                           const CookieIface* cookie,
                           const AddStatFn& add_stat) {
    std::array<char, 80> statname{};

    try {
        std::string bucketName = t.getTaskableName();
        if (!bucketName.empty()) {
            checked_snprintf(
                    statname.data(), statname.size(), "%s:bucket", prefix);
            add_casted_stat(
                    statname.data(), bucketName.c_str(), add_stat, cookie);
        }

        checked_snprintf(statname.data(), statname.size(), "%s:state", prefix);
        add_casted_stat(
                statname.data(), t.getStateName().c_str(), add_stat, cookie);
        checked_snprintf(statname.data(), statname.size(), "%s:task", prefix);
        add_casted_stat(statname.data(), t.getTaskName(), add_stat, cookie);

        if (strcmp(t.getStateName().c_str(), "running") == 0) {
            checked_snprintf(
                    statname.data(), statname.size(), "%s:runtime", prefix);
            const auto duration =
                    std::chrono::steady_clock::now() - t.getTaskStart();
            add_casted_stat(
                    statname.data(),
                    std::chrono::duration_cast<std::chrono::microseconds>(
                            duration)
                            .count(),
                    add_stat,
                    cookie);
        }
        checked_snprintf(
                statname.data(), statname.size(), "%s:cur_time", prefix);
        add_casted_stat(statname.data(),
                        to_ns_since_epoch(t.getCurTime()).count(),
                        add_stat,
                        cookie);
    } catch (std::exception& error) {
        EP_LOG_WARN("addWorkerStats: Failed to build stats: {}", error.what());
    }
}

void CB3ExecutorPool::doWorkerStat(Taskable& taskable,
                                   const CookieIface* cookie,
                                   const AddStatFn& add_stat) {
    if (taskable.isShutdown()) {
        return;
    }

    NonBucketAllocationGuard guard;
    std::lock_guard<std::mutex> lh(tMutex);
    // TODO: implement tracking per engine stats ..
    for (auto& tidx : threadQ) {
        addWorkerStats(tidx->getName().c_str(), *tidx, cookie, add_stat);
    }
}

void CB3ExecutorPool::doTasksStat(Taskable& taskable,
                                  const CookieIface* cookie,
                                  const AddStatFn& add_stat) {
    if (taskable.isShutdown()) {
        return;
    }

    NonBucketAllocationGuard guard;

    std::map<size_t, TaskQpair> taskLocatorCopy;

    {
        // Holding this lock will block scheduling new tasks and cancelling
        // tasks, but threads can still take up work other than this
        std::lock_guard<std::mutex> lh(tMutex);

        // Copy taskLocator
        taskLocatorCopy = taskLocator;
    }

    std::array<char, 80> statname{};
    const char* prefix = "ep_tasks";

    nlohmann::json list = nlohmann::json::array();

    for (auto& pair : taskLocatorCopy) {
        size_t tid = pair.first;
        ExTask& task = pair.second.first;

        if (task->getTaskable().getGID() != taskable.getGID()) {
            continue;
        }
        nlohmann::json obj;

        obj["tid"] = tid;
        obj["state"] = to_string(task->getState());
        obj["name"] = GlobalTask::getTaskName(task->getTaskId());
        obj["this"] = cb::to_hex(reinterpret_cast<uint64_t>(task.get()));
        obj["bucket"] = task->getTaskable().getName();
        obj["description"] = task->getDescription();
        obj["priority"] = task->getQueuePriority();
        obj["waketime_ns"] = task->getWaketime().time_since_epoch().count();
        obj["total_runtime_ns"] = task->getTotalRuntime().count();
        obj["last_starttime_ns"] =
                to_ns_since_epoch(task->getLastStartTime()).count();
        obj["previous_runtime_ns"] = task->getPrevRuntime().count();
        obj["num_runs"] = task->getRunCount();
        obj["type"] = to_string(GlobalTask::getTaskType(task->getTaskId()));

        list.push_back(obj);
    }

    checked_snprintf(statname.data(), statname.size(), "%s:tasks", prefix);
    add_casted_stat(statname.data(), list.dump(), add_stat, cookie);

    checked_snprintf(statname.data(), statname.size(), "%s:cur_time", prefix);
    add_casted_stat(statname.data(),
                    to_ns_since_epoch(std::chrono::steady_clock::now()).count(),
                    add_stat,
                    cookie);

    checked_snprintf(statname.data(), statname.size(), "%s:uptime_s", prefix);
    add_casted_stat(statname.data(), ep_current_time(), add_stat, cookie);

    // It is possible that elements of `tasks` are now the last reference to
    // a GlobalTask, if the GlobalTask was cancelled while this function was
    // running. As such, we need to ensure that if the task is deleted, its
    // memory is accounted to the correct bucket.
    for (auto& pair : taskLocatorCopy) {
        auto& task = pair.second.first;
        auto* engine = task->getEngine();
        BucketAllocationGuard guard(engine);
        task.reset();
    }
}

void CB3ExecutorPool::_stopAndJoinThreads() {
    // Ask all threads to stop (but don't wait)
    for (auto& thread : threadQ) {
        thread->stop(false);
    }

    // Go over all tasks and wake them up.
    for (auto tq : lpTaskQ) {
        size_t wakeAll = threadQ.size();
        tq->doWake(wakeAll);
    }
    for (auto tq : hpTaskQ) {
        size_t wakeAll = threadQ.size();
        tq->doWake(wakeAll);
    }

    // Now reap/join those threads.
    for (auto& thread : threadQ) {
        thread->stop(true);
    }
}
