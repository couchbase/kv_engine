/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2013 Couchbase, Inc.
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

#include "executorpool.h"
#include "bucket_logger.h"
#include "ep_engine.h"
#include "ep_time.h"
#include "executorthread.h"
#include "statistics/collector.h"
#include "taskqueue.h"

#include <nlohmann/json.hpp>
#include <platform/checked_snprintf.h>
#include <platform/string_hex.h>
#include <platform/sysinfo.h>
#include <algorithm>
#include <chrono>
#include <queue>
#include <sstream>

std::mutex ExecutorPool::initGuard;
std::atomic<ExecutorPool*> ExecutorPool::instance;

static const size_t EP_MIN_NONIO_THREADS = 2;

static const size_t EP_MAX_AUXIO_THREADS  = 8;
static const size_t EP_MAX_NONIO_THREADS  = 8;

size_t ExecutorPool::getNumNonIO() {
    // 1. compute: 30% of total threads
    size_t count = maxGlobalThreads * 0.3;

    // 2. adjust computed value to be within range
    count = std::min(EP_MAX_NONIO_THREADS,
                     std::max(EP_MIN_NONIO_THREADS, count));

    // 3. pick user's value if specified
    if (numWorkers[NONIO_TASK_IDX]) {
        count = numWorkers[NONIO_TASK_IDX];
    }
    return count;
}

size_t ExecutorPool::getNumAuxIO() {
    // 1. compute: ceil of 10% of total threads
    size_t count = maxGlobalThreads / 10;
    if (!count || maxGlobalThreads % 10) {
        count++;
    }
    // 2. adjust computed value to be within range
    if (count > EP_MAX_AUXIO_THREADS) {
        count = EP_MAX_AUXIO_THREADS;
    }
    // 3. Override with user's value if specified
    if (numWorkers[AUXIO_TASK_IDX]) {
        count = numWorkers[AUXIO_TASK_IDX];
    }
    return count;
}

size_t ExecutorPool::getNumWriters() {
    return calcNumWriters(
            ThreadPoolConfig::ThreadCount(numWorkers[WRITER_TASK_IDX].load()));
}

size_t ExecutorPool::getNumReaders() {
    return calcNumReaders(
            ThreadPoolConfig::ThreadCount(numWorkers[READER_TASK_IDX].load()));
}

ExecutorPool *ExecutorPool::get() {
    auto* tmp = instance.load();
    if (tmp == nullptr) {
        LockHolder lh(initGuard);
        tmp = instance.load();
        if (tmp == nullptr) {
            // Double-checked locking if instance is null - ensure two threads
            // don't both create an instance.

            Configuration &config =
                ObjectRegistry::getCurrentEngine()->getConfiguration();
            NonBucketAllocationGuard guard;
            tmp = new ExecutorPool(
                    config.getMaxThreads(),
                    NUM_TASK_GROUPS,
                    ThreadPoolConfig::ThreadCount(config.getNumReaderThreads()),
                    ThreadPoolConfig::ThreadCount(config.getNumWriterThreads()),
                    config.getNumAuxioThreads(),
                    config.getNumNonioThreads());
            instance.store(tmp);
        }
    }
    return tmp;
}

void ExecutorPool::shutdown() {
    std::lock_guard<std::mutex> lock(initGuard);
    auto* tmp = instance.load();
    if (tmp != nullptr) {
        NonBucketAllocationGuard guard;
        delete tmp;
        instance = nullptr;
    }
}

ExecutorPool::ExecutorPool(size_t maxThreads,
                           size_t nTaskSets,
                           ThreadPoolConfig::ThreadCount maxReaders,
                           ThreadPoolConfig::ThreadCount maxWriters,
                           size_t maxAuxIO,
                           size_t maxNonIO)
    : numTaskSets(nTaskSets),
      maxGlobalThreads(maxThreads ? maxThreads
                                  : Couchbase::get_available_cpu_count()),
      totReadyTasks(0),
      isHiPrioQset(false),
      isLowPrioQset(false),
      numBuckets(0),
      numSleepers(0),
      curWorkers(nTaskSets),
      numWorkers(nTaskSets),
      numReadyTasks(nTaskSets) {
    for (size_t i = 0; i < nTaskSets; i++) {
        curWorkers[i] = 0;
        numReadyTasks[i] = 0;
    }
    numWorkers[WRITER_TASK_IDX] = static_cast<int>(maxWriters);
    numWorkers[READER_TASK_IDX] = static_cast<int>(maxReaders);
    numWorkers[AUXIO_TASK_IDX] = maxAuxIO;
    numWorkers[NONIO_TASK_IDX] = maxNonIO;
}

ExecutorPool::~ExecutorPool() {
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

TaskQueue *ExecutorPool::_nextTask(ExecutorThread &t, uint8_t tick) {
    if (!tick) {
        return nullptr;
    }

    task_type_t myq = t.taskType;
    TaskQueue *checkQ; // which TaskQueue set should be polled first
    TaskQueue *checkNextQ; // which set of TaskQueue should be polled next
    TaskQueue *toggle = nullptr;
    if ( !(tick % LOW_PRIORITY_FREQ)) { // if only 1 Q set, both point to it
        checkQ = isLowPrioQset ? lpTaskQ[myq] :
                (isHiPrioQset ? hpTaskQ[myq] : nullptr);
        checkNextQ = isHiPrioQset ? hpTaskQ[myq] : checkQ;
    } else {
        checkQ = isHiPrioQset ? hpTaskQ[myq] :
                (isLowPrioQset ? lpTaskQ[myq] : nullptr);
        checkNextQ = isLowPrioQset ? lpTaskQ[myq] : checkQ;
    }
    while (t.state == EXECUTOR_RUNNING) {
        if (checkQ && checkQ->fetchNextTask(t)) {
            return checkQ;
        }
        if (toggle || checkQ == checkNextQ) {
            TaskQueue *sleepQ = getSleepQ(myq);
            if (sleepQ->sleepThenFetchNextTask(t)) {
                return sleepQ;
            } else {
                return NULL;
            }
        }
        toggle = checkQ;
        checkQ = checkNextQ;
        checkNextQ = toggle;
    }
    return nullptr;
}

TaskQueue *ExecutorPool::nextTask(ExecutorThread &t, uint8_t tick) {
    TaskQueue *tq = _nextTask(t, tick);
    return tq;
}

void ExecutorPool::addWork(size_t newWork, task_type_t qType) {
    if (newWork) {
        totReadyTasks.fetch_add(newWork);
        numReadyTasks[qType].fetch_add(newWork);
    }
}

void ExecutorPool::lessWork(task_type_t qType) {
    if (numReadyTasks[qType].load() == 0) {
        throw std::logic_error("ExecutorPool::lessWork: number of ready "
                "tasks on qType " + std::to_string(qType) + " is zero");
    }
    numReadyTasks[qType]--;
    totReadyTasks--;
}

void ExecutorPool::startWork(task_type_t taskType) {
    if (taskType == NO_TASK_TYPE || taskType == NUM_TASK_GROUPS) {
        throw std::logic_error(
                "ExecutorPool::startWork: worker is starting task with invalid "
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

void ExecutorPool::doneWork(task_type_t taskType) {
    if (taskType == NO_TASK_TYPE || taskType == NUM_TASK_GROUPS) {
        throw std::logic_error(
                "ExecutorPool::doneWork: worker is finishing task with invalid "
                "type {" + std::to_string(taskType) + "}");
    } else {
        --curWorkers[taskType];
        // Record that a thread is done working on a particular queue type
        EP_LOG_TRACE("Done with task type:{{{}}} capacity:{{{}}}",
                     taskType,
                     numWorkers[taskType].load());
    }
}

bool ExecutorPool::_cancel(size_t taskId, bool eraseTask) {
    LockHolder lh(tMutex);
    auto itr = taskLocator.find(taskId);
    if (itr == taskLocator.end()) {
        EP_LOG_DEBUG("Task id {} not found", uint64_t(taskId));
        return false;
    }

    ExTask task = itr->second.first;
    EP_LOG_TRACE("Cancel task {} id {} on bucket {} {}",
                 task->getDescription(),
                 task->getId(),
                 task->getTaskable().getName(),
                 eraseTask ? "final erase" : "!");

    task->cancel(); // must be idempotent, just set state to dead

    if (eraseTask) { // only internal threads can erase tasks
        if (!task->isdead()) {
            throw std::logic_error("ExecutorPool::_cancel: task '" +
                                   task->getDescription() +
                                   "' is not dead after calling "
                                   "cancel() on it");
        }
        taskLocator.erase(itr);
        {
            // Account the task reset to its engine
            BucketAllocationGuard guard(task->getEngine());
            task.reset();
        }
        tMutex.notify_all();
    } else { // wake up the task from the TaskQ so a thread can safely erase it
             // otherwise we may race with unregisterTaskable where a unlocated
             // task runs in spite of its bucket getting unregistered
        itr->second.second->wake(task);
    }
    return true;
}

bool ExecutorPool::cancel(size_t taskId, bool eraseTask) {
    NonBucketAllocationGuard guard;
    bool rv = _cancel(taskId, eraseTask);
    return rv;
}

bool ExecutorPool::_wake(size_t taskId) {
    LockHolder lh(tMutex);
    auto itr = taskLocator.find(taskId);
    if (itr != taskLocator.end()) {
        itr->second.second->wake(itr->second.first);
        return true;
    }
    return false;
}

bool ExecutorPool::wake(size_t taskId) {
    NonBucketAllocationGuard guard;
    bool rv = _wake(taskId);
    return rv;
}

bool ExecutorPool::_snooze(size_t taskId, double toSleep) {
    LockHolder lh(tMutex);
    auto itr = taskLocator.find(taskId);
    if (itr != taskLocator.end()) {
        itr->second.second->snooze(itr->second.first, toSleep);
        return true;
    }
    return false;
}

bool ExecutorPool::snooze(size_t taskId, double toSleep) {
    NonBucketAllocationGuard guard;
    bool rv = _snooze(taskId, toSleep);
    return rv;
}

TaskQueue* ExecutorPool::_getTaskQueue(const Taskable& t,
                                       task_type_t qidx) {
    TaskQueue         *q             = nullptr;
    size_t            curNumThreads  = 0;

    bucket_priority_t bucketPriority = t.getWorkloadPriority();

    if (qidx < 0 || static_cast<size_t>(qidx) >= numTaskSets) {
        throw std::invalid_argument("ExecutorPool::_getTaskQueue: qidx "
                "(which is " + std::to_string(qidx) + ") is outside the range [0,"
                + std::to_string(numTaskSets) + ")");
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
                throw std::logic_error("ExecutorPool::_getTaskQueue: At "
                        "maximum capacity but low-priority taskQ size "
                        "(which is " + std::to_string(lpTaskQ.size()) +
                        ") is not " + std::to_string(numTaskSets));
            }
            q = lpTaskQ[qidx];
            break;

        case HIGH_BUCKET_PRIORITY:
            if (hpTaskQ.size() != numTaskSets) {
                throw std::logic_error("ExecutorPool::_getTaskQueue: At "
                        "maximum capacity but high-priority taskQ size "
                        "(which is " + std::to_string(lpTaskQ.size()) +
                        ") is not " + std::to_string(numTaskSets));
            }
            q = hpTaskQ[qidx];
            break;

        default:
            throw std::logic_error("ExecutorPool::_getTaskQueue: Invalid "
                    "bucketPriority " + std::to_string(bucketPriority));
        }
    }
    return q;
}

size_t ExecutorPool::_schedule(ExTask task) {
    LockHolder lh(tMutex);
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

size_t ExecutorPool::schedule(ExTask task) {
    NonBucketAllocationGuard guard;
    size_t rv = _schedule(task);
    return rv;
}

void ExecutorPool::_registerTaskable(Taskable& taskable) {
    TaskQ *taskQ;
    bool *whichQset;
    const char *queueName;
    WorkLoadPolicy &workload = taskable.getWorkLoadPolicy();
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
        LockHolder lh(tMutex);

        if (!(*whichQset)) {
            taskQ->reserve(numTaskSets);
            for (size_t i = 0; i < numTaskSets; ++i) {
                taskQ->push_back(
                        new TaskQueue(this, (task_type_t)i, queueName));
            }
            *whichQset = true;
        }

        taskOwners.insert(&taskable);
        numBuckets++;
    }

    _startWorkers();
}

void ExecutorPool::registerTaskable(Taskable& taskable) {
    NonBucketAllocationGuard guard;
    _registerTaskable(taskable);
}

ssize_t ExecutorPool::_adjustWorkers(task_type_t type, size_t desiredNumItems) {
    std::string typeName{to_string(type)};

    // vector of threads which have been stopped
    // and should be joined after unlocking, if any.
    ThreadQ removed;

    size_t numItems;

    {
        // Lock mutex, we are modifying threadQ
        LockHolder lh(tMutex);

        // How many threads performing this task type there are currently
        numItems = std::count_if(
                threadQ.begin(), threadQ.end(), [type](ExecutorThread* thread) {
                    return thread->taskType == type;
                });

        if (numItems == desiredNumItems) {
            return 0;
        }

        EP_LOG_INFO("Adjusting threads of type:{} from:{} to:{}",
                    typeName,
                    numItems,
                    desiredNumItems);

        if (numItems < desiredNumItems) {
            // If we want to increase the number of threads, they must be
            // created and started
            for (size_t tidx = numItems; tidx < desiredNumItems; ++tidx) {
                threadQ.push_back(new ExecutorThread(
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
                    removed.push_back(*itr);

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
        delete (*itr);
        itr = removed.erase(itr);
    }

    return ssize_t(desiredNumItems) - ssize_t(numItems);
}

void ExecutorPool::adjustWorkers(task_type_t type, size_t newCount) {
    NonBucketAllocationGuard guard;
    _adjustWorkers(type, newCount);
}

bool ExecutorPool::_startWorkers() {
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

std::vector<ExTask> ExecutorPool::_stopTaskGroup(
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

std::vector<ExTask> ExecutorPool::_unregisterTaskable(Taskable& taskable,
                                                      bool force) {
    EP_LOG_INFO("Unregistering {} taskable {}",
                (numBuckets == 1) ? "last" : "",
                taskable.getName());

    std::unique_lock<std::mutex> lh(tMutex);

    auto rv = _stopTaskGroup(taskable.getGID(), lh, force);

    taskOwners.erase(&taskable);
    if (!(--numBuckets)) {
        if (!taskLocator.empty()) {
            throw std::logic_error("ExecutorPool::_unregisterTaskable: "
                    "Attempting to unregister taskable '" +
                    taskable.getName() + "' but taskLocator is not empty");
        }
        for (auto& tidx : threadQ) {
            tidx->stop(false); // only set state to DEAD
        }

        for (unsigned int idx = 0; idx < numTaskSets; idx++) {
            TaskQueue *sleepQ = getSleepQ(idx);
            size_t wakeAll = threadQ.size();
            sleepQ->doWake(wakeAll);
        }

        for (auto& tidx : threadQ) {
            tidx->stop(/*wait for threads */);
            delete tidx;
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

std::vector<ExTask> ExecutorPool::unregisterTaskable(Taskable& taskable,
                                                     bool force) {
    // Note: unregistering a bucket is special - any memory allocations /
    // deallocations made while unregistering *should* be accounted to the
    // bucket in question - hence no `onSwitchThread(NULL)` call.
    return _unregisterTaskable(taskable, force);
}

void ExecutorPool::doTaskQStat(EventuallyPersistentEngine* engine,
                               const void* cookie,
                               const AddStatFn& add_stat) {
    if (engine->getEpStats().isShutdown) {
        return;
    }

    NonBucketAllocationGuard guard;
    try {
        char statname[80] = {0};
        if (isHiPrioQset) {
            for (size_t i = 0; i < numTaskSets; i++) {
                checked_snprintf(statname, sizeof(statname),
                                 "ep_workload:%s:InQsize",
                                 hpTaskQ[i]->getName().c_str());
                add_casted_stat(statname, hpTaskQ[i]->getFutureQueueSize(),
                                add_stat,
                                cookie);
                checked_snprintf(statname, sizeof(statname),
                                 "ep_workload:%s:OutQsize",
                                 hpTaskQ[i]->getName().c_str());
                add_casted_stat(statname, hpTaskQ[i]->getReadyQueueSize(),
                                add_stat,
                                cookie);
            }
        }
        if (isLowPrioQset) {
            for (size_t i = 0; i < numTaskSets; i++) {
                checked_snprintf(statname, sizeof(statname),
                                 "ep_workload:%s:InQsize",
                                 lpTaskQ[i]->getName().c_str());
                add_casted_stat(statname, lpTaskQ[i]->getFutureQueueSize(),
                                add_stat,
                                cookie);
                checked_snprintf(statname, sizeof(statname),
                                 "ep_workload:%s:OutQsize",
                                 lpTaskQ[i]->getName().c_str());
                add_casted_stat(statname, lpTaskQ[i]->getReadyQueueSize(),
                                add_stat,
                                cookie);
            }
        }
    } catch (std::exception& error) {
        EP_LOG_WARN("ExecutorPool::doTaskQStat: Failed to build stats: {}",
                    error.what());
    }
}

static void addWorkerStats(const char* prefix,
                           ExecutorThread* t,
                           const void* cookie,
                           const AddStatFn& add_stat) {
    char statname[80] = {0};

    try {
        std::string bucketName = t->getTaskableName();
        if (!bucketName.empty()) {
            checked_snprintf(statname, sizeof(statname), "%s:bucket", prefix);
            add_casted_stat(statname, bucketName.c_str(), add_stat, cookie);
        }

        checked_snprintf(statname, sizeof(statname), "%s:state", prefix);
        add_casted_stat(statname, t->getStateName().c_str(), add_stat, cookie);
        checked_snprintf(statname, sizeof(statname), "%s:task", prefix);
        add_casted_stat(statname, t->getTaskName(), add_stat, cookie);

        if (strcmp(t->getStateName().c_str(), "running") == 0) {
            checked_snprintf(statname, sizeof(statname), "%s:runtime", prefix);
            const auto duration =
                    std::chrono::steady_clock::now() - t->getTaskStart();
            add_casted_stat(statname, std::chrono::duration_cast<
                            std::chrono::microseconds>(duration).count(),
                            add_stat, cookie);
        }
        checked_snprintf(statname, sizeof(statname), "%s:cur_time", prefix);
        add_casted_stat(statname, to_ns_since_epoch(t->getCurTime()).count(),
                        add_stat, cookie);
    } catch (std::exception& error) {
        EP_LOG_WARN("addWorkerStats: Failed to build stats: {}", error.what());
    }
}

void ExecutorPool::doWorkerStat(EventuallyPersistentEngine* engine,
                                const void* cookie,
                                const AddStatFn& add_stat) {
    if (engine->getEpStats().isShutdown) {
        return;
    }

    NonBucketAllocationGuard guard;
    LockHolder lh(tMutex);
    //TODO: implement tracking per engine stats ..
    for (auto& tidx : threadQ) {
        addWorkerStats(tidx->getName().c_str(), tidx, cookie, add_stat);
    }
}

void ExecutorPool::doTasksStat(EventuallyPersistentEngine* engine,
                               const void* cookie,
                               const AddStatFn& add_stat) {
    if (engine->getEpStats().isShutdown) {
        return;
    }

    NonBucketAllocationGuard guard;

    std::map<size_t, TaskQpair> taskLocatorCopy;

    {
        // Holding this lock will block scheduling new tasks and cancelling
        // tasks, but threads can still take up work other than this
        LockHolder lh(tMutex);

        // Copy taskLocator
        taskLocatorCopy = taskLocator;
    }

    char statname[80] = {0};
    char prefix[] = "ep_tasks";

    nlohmann::json list = nlohmann::json::array();

    for (auto& pair : taskLocatorCopy) {
        size_t tid = pair.first;
        ExTask& task = pair.second.first;

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
        obj["num_runs"] =
                engine->getEpStats()
                        .taskRuntimeHisto[static_cast<int>(task->getTaskId())]
                        .getValueCount();
        obj["type"] = TaskQueue::taskType2Str(
                GlobalTask::getTaskType(task->getTaskId()));

        list.push_back(obj);
    }

    checked_snprintf(statname, sizeof(statname), "%s:tasks", prefix);
    add_casted_stat(statname, list.dump(), add_stat, cookie);

    checked_snprintf(statname, sizeof(statname), "%s:cur_time", prefix);
    add_casted_stat(statname,
                    to_ns_since_epoch(std::chrono::steady_clock::now()).count(),
                    add_stat,
                    cookie);

    checked_snprintf(statname, sizeof(statname), "%s:uptime_s", prefix);
    add_casted_stat(statname, ep_current_time(), add_stat, cookie);
}

void ExecutorPool::_stopAndJoinThreads() {

    // Ask all threads to stop (but don't wait)
    for (auto thread : threadQ) {
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
    for (auto thread : threadQ) {
        thread->stop(true);
    }
}

size_t ExecutorPool::calcNumReaders(
        ThreadPoolConfig::ThreadCount threadCount) const {
    switch (threadCount) {
    case ThreadPoolConfig::ThreadCount::Default: {
        // Default: configure Reader threads based on CPU count; constraining
        // to between 4 and 16 threads (relatively conservative number).
        auto readers = maxGlobalThreads;
        readers = std::min(readers, size_t{16});
        readers = std::max(readers, size_t{4});
        return readers;
    }

    case ThreadPoolConfig::ThreadCount::DiskIOOptimized: {
        // Configure Reader threads based on CPU count; increased up
        // to a maximum of 64 threads.

        // Note: For maximum IO throughput we should create as many Reader
        // threads as concurrent iops the system can support, given we use
        // synchronous (blocking) IO and hence could utilise more threads than
        // CPU cores. However, knowing the number of concurrent IOPs the system
        // can support is hard, so we use #CPUs as a proxy for it - machines
        // with lots of CPU cores are more likely to have more IO than little
        // machines.
        // However given we don't have test environments larger than
        // 64 cores, limit to 64.
        auto readers = maxGlobalThreads;
        readers = std::min(readers, size_t{64});
        readers = std::max(readers, size_t{4});
        return readers;
    }

    default:
        // User specified an explicit value - use that unmodified.
        return static_cast<size_t>(threadCount);
    }
}

size_t ExecutorPool::calcNumWriters(
        ThreadPoolConfig::ThreadCount threadCount) const {
    switch (threadCount) {
    case ThreadPoolConfig::ThreadCount::Default:
        // Default: configure Writer threads to 4 (default from v3.0 onwards).
        return 4;

    case ThreadPoolConfig::ThreadCount::DiskIOOptimized: {
        // Configure Writer threads based on CPU count; up to a maximum of 64
        // threads.

        // Note: For maximum IO throughput we should create as many Writer
        // threads as concurrent iops the system can support, given we use
        // synchronous (blocking) IO and hence could utilise more threads than
        // CPU cores. However, knowing the number of concurrent IOPs the system
        // can support is hard, so we use #CPUs as a proxy for it - machines
        // with lots of CPU cores are more likely to have more IO than little
        // machines. However given we don't have test environments larger than
        // 64 cores, limit to 64.
        auto writers = maxGlobalThreads;
        writers = std::min(writers, size_t{64});
        writers = std::max(writers, size_t{4});
        return writers;
    }

    default:
        // User specified an explicit value - use that unmodified.
        return static_cast<size_t>(threadCount);
    }
}
