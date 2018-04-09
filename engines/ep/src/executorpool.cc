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

#include "config.h"

#include "ep_engine.h"
#include "ep_time.h"
#include "executorpool.h"
#include "executorthread.h"
#include "statwriter.h"
#include "taskqueue.h"

#include <cJSON_utils.h>
#include <platform/checked_snprintf.h>
#include <platform/processclock.h>
#include <platform/string.h>
#include <platform/sysinfo.h>
#include <algorithm>
#include <chrono>
#include <queue>
#include <sstream>

std::mutex ExecutorPool::initGuard;
std::atomic<ExecutorPool*> ExecutorPool::instance;

static const size_t EP_MIN_NUM_THREADS    = 10;
static const size_t EP_MIN_READER_THREADS = 4;
static const size_t EP_MIN_WRITER_THREADS = 4;
static const size_t EP_MIN_NONIO_THREADS = 2;


static const size_t EP_MAX_READER_THREADS = 12;
static const size_t EP_MAX_WRITER_THREADS = 8;
static const size_t EP_MAX_AUXIO_THREADS  = 8;
static const size_t EP_MAX_NONIO_THREADS  = 8;

size_t ExecutorPool::getNumNonIO(void) {
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

size_t ExecutorPool::getNumAuxIO(void) {
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

size_t ExecutorPool::getNumWriters(void) {
    size_t count = 0;
    // 1. compute: floor of Half of what remains after nonIO, auxIO threads
    if (maxGlobalThreads > (getNumAuxIO() + getNumNonIO())) {
        count = maxGlobalThreads - getNumAuxIO() - getNumNonIO();
        count = count >> 1;
    }
    // 2. adjust computed value to be within range
    if (count > EP_MAX_WRITER_THREADS) {
        count = EP_MAX_WRITER_THREADS;
    } else if (count < EP_MIN_WRITER_THREADS) {
        count = EP_MIN_WRITER_THREADS;
    }
    // 3. Override with user's value if specified
    if (numWorkers[WRITER_TASK_IDX]) {
        count = numWorkers[WRITER_TASK_IDX];
    }
    return count;
}

size_t ExecutorPool::getNumReaders(void) {
    size_t count = 0;
    // 1. compute: what remains after writers, nonIO & auxIO threads are taken
    if (maxGlobalThreads >
            (getNumWriters() + getNumAuxIO() + getNumNonIO())) {
        count = maxGlobalThreads
              - getNumWriters() - getNumAuxIO() - getNumNonIO();
    }
    // 2. adjust computed value to be within range
    if (count > EP_MAX_READER_THREADS) {
        count = EP_MAX_READER_THREADS;
    } else if (count < EP_MIN_READER_THREADS) {
        count = EP_MIN_READER_THREADS;
    }
    // 3. Override with user's value if specified
    if (numWorkers[READER_TASK_IDX]) {
        count = numWorkers[READER_TASK_IDX];
    }
    return count;
}

size_t numShards;

ExecutorPool *ExecutorPool::get(void) {
    auto* tmp = instance.load();
    if (tmp == nullptr) {
        LockHolder lh(initGuard);
        tmp = instance.load();
        if (tmp == nullptr) {
            // Double-checked locking if instance is null - ensure two threads
            // don't both create an instance.

            Configuration &config =
                ObjectRegistry::getCurrentEngine()->getConfiguration();
            EventuallyPersistentEngine *epe =
                                   ObjectRegistry::onSwitchThread(NULL, true);
            tmp = new ExecutorPool(config.getMaxThreads(),
                                   NUM_TASK_GROUPS,
                                   config.getNumReaderThreads(),
                                   config.getNumWriterThreads(),
                                   config.getNumAuxioThreads(),
                                   config.getNumNonioThreads());
			numShards = config.getMaxNumShards();
            ObjectRegistry::onSwitchThread(epe);
            instance.store(tmp);
        }
    }
    return tmp;
}

void ExecutorPool::shutdown(void) {
    std::lock_guard<std::mutex> lock(initGuard);
    auto* tmp = instance.load();
    if (tmp != nullptr) {
        delete tmp;
        instance = nullptr;
    }
}

ExecutorPool::ExecutorPool(size_t maxThreads, size_t nTaskSets,
                           size_t maxReaders, size_t maxWriters,
                           size_t maxAuxIO,   size_t maxNonIO) :
                  numTaskSets(nTaskSets), totReadyTasks(0),
                  isHiPrioQset(false), isLowPrioQset(false), numBuckets(0),
                  numSleepers(0), curWorkers(nTaskSets), numWorkers(nTaskSets),
                  numReadyTasks(nTaskSets) {
    size_t numCPU = Couchbase::get_available_cpu_count();
    size_t numThreads = (size_t)((numCPU * 3)/4);
    numThreads = (numThreads < EP_MIN_NUM_THREADS) ?
                        EP_MIN_NUM_THREADS : numThreads;
    maxGlobalThreads = maxThreads ? maxThreads : numThreads;
    for (size_t i = 0; i < nTaskSets; i++) {
        curWorkers[i] = 0;
        numReadyTasks[i] = 0;
    }
    numWorkers[WRITER_TASK_IDX] = maxWriters;
    numWorkers[READER_TASK_IDX] = maxReaders;
    numWorkers[AUXIO_TASK_IDX] = maxAuxIO;
    numWorkers[NONIO_TASK_IDX] = maxNonIO;
}

ExecutorPool::~ExecutorPool(void) {
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
        return NULL;
    }

    task_type_t myq = t.taskType;
    TaskQueue *checkQ; // which TaskQueue set should be polled first
    TaskQueue *checkNextQ; // which set of TaskQueue should be polled next
    TaskQueue *toggle = NULL;
    if ( !(tick % LOW_PRIORITY_FREQ)) { // if only 1 Q set, both point to it
        checkQ = isLowPrioQset ? lpTaskQ[myq] :
                (isHiPrioQset ? hpTaskQ[myq] : NULL);
        checkNextQ = isHiPrioQset ? hpTaskQ[myq] : checkQ;
    } else {
        checkQ = isHiPrioQset ? hpTaskQ[myq] :
                (isLowPrioQset ? lpTaskQ[myq] : NULL);
        checkNextQ = isLowPrioQset ? lpTaskQ[myq] : checkQ;
    }
    while (t.state == EXECUTOR_RUNNING) {
        if (checkQ &&
            checkQ->fetchNextTask(t, false)) {
            return checkQ;
        }
        if (toggle || checkQ == checkNextQ) {
            TaskQueue *sleepQ = getSleepQ(myq);
            if (sleepQ->fetchNextTask(t, true)) {
                return sleepQ;
            } else {
                return NULL;
            }
        }
        toggle = checkQ;
        checkQ = checkNextQ;
        checkNextQ = toggle;
    }
    return NULL;
}

TaskQueue *ExecutorPool::nextTask(ExecutorThread &t, uint8_t tick) {
    EventuallyPersistentEngine *epe = ObjectRegistry::onSwitchThread(NULL, true);
    TaskQueue *tq = _nextTask(t, tick);
    ObjectRegistry::onSwitchThread(epe);
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
        LOG(EXTENSION_LOG_DEBUG,
            "Taking up work in task "
            "type:{%" PRIu32 "} "
            "current:{%" PRIu16 "}, max:{%" PRIu16 "}",
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
        LOG(EXTENSION_LOG_DEBUG,
            "Done with task type:{%" PRIu32 "} capacity:{%" PRIu16 "}",
            taskType,
            numWorkers[taskType].load());
    }
}

bool ExecutorPool::_cancel(size_t taskId, bool eraseTask) {
    LockHolder lh(tMutex);
    std::map<size_t, TaskQpair>::iterator itr = taskLocator.find(taskId);
    if (itr == taskLocator.end()) {
        LOG(EXTENSION_LOG_DEBUG, "Task id %" PRIu64 " not found",
            uint64_t(taskId));
        return false;
    }

    ExTask task = itr->second.first;
    LOG(EXTENSION_LOG_DEBUG,
        "Cancel task %.*s id %" PRIu64 " on bucket %s %s",
        int(task->getDescription().size()),
        task->getDescription().data(),
        uint64_t(task->getId()),
        task->getTaskable().getName().c_str(),
        eraseTask ? "final erase" : "!");

    task->cancel(); // must be idempotent, just set state to dead

    if (eraseTask) { // only internal threads can erase tasks
        if (!task->isdead()) {
            throw std::logic_error("ExecutorPool::_cancel: task '" +
                                   to_string(task->getDescription()) +
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
    return true;
}

bool ExecutorPool::cancel(size_t taskId, bool eraseTask) {
    EventuallyPersistentEngine *epe = ObjectRegistry::onSwitchThread(NULL, true);
    bool rv = _cancel(taskId, eraseTask);
    ObjectRegistry::onSwitchThread(epe);
    return rv;
}

bool ExecutorPool::_wake(size_t taskId) {
    LockHolder lh(tMutex);
    std::map<size_t, TaskQpair>::iterator itr = taskLocator.find(taskId);
    if (itr != taskLocator.end()) {
        itr->second.second->wake(itr->second.first);
        return true;
    }
    return false;
}

bool ExecutorPool::wake(size_t taskId) {
    EventuallyPersistentEngine *epe = ObjectRegistry::onSwitchThread(NULL, true);
    bool rv = _wake(taskId);
    ObjectRegistry::onSwitchThread(epe);
    return rv;
}

bool ExecutorPool::_snooze(size_t taskId, double toSleep) {
    LockHolder lh(tMutex);
    std::map<size_t, TaskQpair>::iterator itr = taskLocator.find(taskId);
    if (itr != taskLocator.end()) {
        itr->second.second->snooze(itr->second.first, toSleep);
        return true;
    }
    return false;
}

bool ExecutorPool::snooze(size_t taskId, double toSleep) {
    EventuallyPersistentEngine *epe = ObjectRegistry::onSwitchThread(NULL, true);
    bool rv = _snooze(taskId, toSleep);
    ObjectRegistry::onSwitchThread(epe);
    return rv;
}

TaskQueue* ExecutorPool::_getTaskQueue(const Taskable& t,
                                       task_type_t qidx) {
    TaskQueue         *q             = NULL;
    size_t            curNumThreads  = 0;

    bucket_priority_t bucketPriority = t.getWorkloadPriority();

    if (qidx < 0 || static_cast<size_t>(qidx) >= numTaskSets) {
        throw std::invalid_argument("ExecutorPool::_getTaskQueue: qidx "
                "(which is " + std::to_string(qidx) + ") is outside the range [0,"
                + std::to_string(numTaskSets) + ")");
    }

    curNumThreads = threadQ.size();

    if (!bucketPriority) {
        LOG(EXTENSION_LOG_WARNING, "Trying to schedule task for unregistered "
            "bucket %s", t.getName().c_str());
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
    EventuallyPersistentEngine *epe = ObjectRegistry::onSwitchThread(NULL, true);
    size_t rv = _schedule(task);
    ObjectRegistry::onSwitchThread(epe);
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
        LOG(EXTENSION_LOG_NOTICE, "Taskable %s registered with low priority",
            taskable.getName().c_str());
    } else {
        taskable.setWorkloadPriority(HIGH_BUCKET_PRIORITY);
        taskQ = &hpTaskQ;
        whichQset = &isHiPrioQset;
        queueName = "HiPrioQ_";
        LOG(EXTENSION_LOG_NOTICE, "Taskable %s registered with high priority",
            taskable.getName().c_str());
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
    EventuallyPersistentEngine *epe = ObjectRegistry::onSwitchThread(NULL, true);
    _registerTaskable(taskable);
    ObjectRegistry::onSwitchThread(epe);
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

        LOG(EXTENSION_LOG_NOTICE,
            "Adjusting threads of type:%s from:%" PRIu64 " to:%" PRIu64,
            typeName.c_str(),
            uint64_t(numItems),
            uint64_t(desiredNumItems));

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
    EventuallyPersistentEngine* epe =
            ObjectRegistry::onSwitchThread(NULL, true);
    _adjustWorkers(type, newCount);
    ObjectRegistry::onSwitchThread(epe);
}

bool ExecutorPool::_startWorkers(void) {
    size_t numReaders = getNumReaders();
    size_t numWriters = getNumWriters();
    size_t numAuxIO = getNumAuxIO();
    size_t numNonIO = getNumNonIO();

    if (!numWorkers[WRITER_TASK_IDX]) {
        // MB-12279: Limit writers to 4 for faster bgfetches in DGM by default
        numWriters = numShards;
        numReaders = numShards;
    }

    _adjustWorkers(READER_TASK_IDX, numReaders);
    _adjustWorkers(WRITER_TASK_IDX, numWriters);
    _adjustWorkers(AUXIO_TASK_IDX, numAuxIO);
    _adjustWorkers(NONIO_TASK_IDX, numNonIO);

    return true;
}

bool ExecutorPool::_stopTaskGroup(task_gid_t taskGID,
                                  task_type_t taskType,
                                  bool force) {
    bool unfinishedTask;
    bool retVal = false;
    std::map<size_t, TaskQpair>::iterator itr;

    std::unique_lock<std::mutex> lh(tMutex);
    do {
        ExTask task;
        unfinishedTask = false;
        for (itr = taskLocator.begin(); itr != taskLocator.end(); itr++) {
            task = itr->second.first;
            TaskQueue *q = itr->second.second;
            if (task->getTaskable().getGID() == taskGID &&
                (taskType == NO_TASK_TYPE || q->queueType == taskType)) {
                LOG(EXTENSION_LOG_NOTICE,
                    "Stopping Task id %" PRIu64 " %s %.*s",
                    uint64_t(task->getId()),
                    task->getTaskable().getName().c_str(),
                    int(task->getDescription().size()),
                    task->getDescription().data());
                // If force flag is set during shutdown, cancel all tasks
                // without considering the blockShutdown status of the task.
                if (force || !task->blockShutdown) {
                    task->cancel(); // Must be idempotent
                }
                q->wake(task);
                unfinishedTask = true;
                retVal = true;
            }
        }
        if (unfinishedTask) {
            tMutex.wait_for(lh, MIN_SLEEP_TIME); // Wait till task gets cancelled
        }
    } while (unfinishedTask);

    return retVal;
}

bool ExecutorPool::stopTaskGroup(task_gid_t taskGID,
                                 task_type_t taskType,
                                 bool force) {
    // Note: Stopping a task group is special - any memory allocations /
    // deallocations made while unregistering *should* be accounted to the
    // bucket in question - hence no `onSwitchThread(NULL)` call.
    return _stopTaskGroup(taskGID, taskType, force);
}

void ExecutorPool::_unregisterTaskable(Taskable& taskable, bool force) {

    LOG(EXTENSION_LOG_NOTICE, "Unregistering %s taskable %s",
            (numBuckets == 1)? "last" : "", taskable.getName().c_str());

    _stopTaskGroup(taskable.getGID(), NO_TASK_TYPE, force);

    LockHolder lh(tMutex);
    taskOwners.erase(&taskable);
    if (!(--numBuckets)) {
        if (taskLocator.size()) {
            throw std::logic_error("ExecutorPool::_unregisterTaskable: "
                    "Attempting to unregister taskable '" +
                    taskable.getName() + "' but taskLocator is not empty");
        }
        for (size_t tidx = 0; tidx < threadQ.size(); ++tidx) {
            threadQ[tidx]->stop(false); // only set state to DEAD
        }

        for (unsigned int idx = 0; idx < numTaskSets; idx++) {
            TaskQueue *sleepQ = getSleepQ(idx);
            size_t wakeAll = threadQ.size();
            sleepQ->doWake(wakeAll);
        }

        for (size_t tidx = 0; tidx < threadQ.size(); ++tidx) {
            threadQ[tidx]->stop(/*wait for threads */);
            delete threadQ[tidx];
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
}

void ExecutorPool::unregisterTaskable(Taskable& taskable, bool force) {
    // Note: unregistering a bucket is special - any memory allocations /
    // deallocations made while unregistering *should* be accounted to the
    // bucket in question - hence no `onSwitchThread(NULL)` call.
    _unregisterTaskable(taskable, force);
}

void ExecutorPool::doTaskQStat(EventuallyPersistentEngine *engine,
                               const void *cookie, ADD_STAT add_stat) {
    if (engine->getEpStats().isShutdown) {
        return;
    }

    EventuallyPersistentEngine *epe = ObjectRegistry::onSwitchThread(NULL, true);
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
                size_t pendingQsize = hpTaskQ[i]->getPendingQueueSize();
                if (pendingQsize > 0) {
                    checked_snprintf(statname, sizeof(statname),
                                     "ep_workload:%s:PendingQ",
                                     hpTaskQ[i]->getName().c_str());
                    add_casted_stat(statname, pendingQsize, add_stat, cookie);
                }
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
                size_t pendingQsize = lpTaskQ[i]->getPendingQueueSize();
                if (pendingQsize > 0) {
                    checked_snprintf(statname, sizeof(statname),
                                     "ep_workload:%s:PendingQ",
                                     lpTaskQ[i]->getName().c_str());
                    add_casted_stat(statname, pendingQsize, add_stat, cookie);
                }
            }
        }
    } catch (std::exception& error) {
        LOG(EXTENSION_LOG_WARNING,
            "ExecutorPool::doTaskQStat: Failed to build stats: %s",
            error.what());
    }
    ObjectRegistry::onSwitchThread(epe);
}

static void showJobLog(const char *logname, const char *prefix,
                       const std::vector<TaskLogEntry> &log,
                       const void *cookie, ADD_STAT add_stat) {
    char statname[80] = {0};
    for (size_t i = 0;i < log.size(); ++i) {
        try {
            checked_snprintf(statname, sizeof(statname), "%s:%s:%d:task",
                             prefix,
                             logname, static_cast<int>(i));
            add_casted_stat(statname, log[i].getName().c_str(), add_stat,
                            cookie);
            checked_snprintf(statname, sizeof(statname), "%s:%s:%d:type",
                             prefix,
                             logname, static_cast<int>(i));
            add_casted_stat(statname,
                            TaskQueue::taskType2Str(
                                log[i].getTaskType()).c_str(),
                            add_stat, cookie);
            checked_snprintf(statname, sizeof(statname), "%s:%s:%d:starttime",
                             prefix, logname, static_cast<int>(i));
            add_casted_stat(statname, log[i].getTimestamp(), add_stat,
                            cookie);
            checked_snprintf(statname, sizeof(statname), "%s:%s:%d:runtime",
                             prefix, logname, static_cast<int>(i));
            const auto duration_ms = std::chrono::duration_cast
                    <std::chrono::microseconds>(log[i].getDuration()).count();
            add_casted_stat(statname, duration_ms, add_stat, cookie);
        } catch (std::exception& error) {
            LOG(EXTENSION_LOG_WARNING,
                "showJobLog: Failed to build stats: %s", error.what());
        }
    }
}

static void addWorkerStats(const char *prefix, ExecutorThread *t,
                           const void *cookie, ADD_STAT add_stat) {
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
            const auto duration = ProcessClock::now() - t->getTaskStart();
            add_casted_stat(statname, std::chrono::duration_cast<
                            std::chrono::microseconds>(duration).count(),
                            add_stat, cookie);
        }
        checked_snprintf(statname, sizeof(statname), "%s:waketime", prefix);
        add_casted_stat(statname, to_ns_since_epoch(t->getWaketime()).count(),
                        add_stat, cookie);
        checked_snprintf(statname, sizeof(statname), "%s:cur_time", prefix);
        add_casted_stat(statname, to_ns_since_epoch(t->getCurTime()).count(),
                        add_stat, cookie);
    } catch (std::exception& error) {
        LOG(EXTENSION_LOG_WARNING,
            "addWorkerStats: Failed to build stats: %s", error.what());
    }
}

void ExecutorPool::doWorkerStat(EventuallyPersistentEngine *engine,
                               const void *cookie, ADD_STAT add_stat) {
    if (engine->getEpStats().isShutdown) {
        return;
    }

    EventuallyPersistentEngine *epe = ObjectRegistry::onSwitchThread(NULL, true);
    LockHolder lh(tMutex);
    //TODO: implement tracking per engine stats ..
    for (size_t tidx = 0; tidx < threadQ.size(); ++tidx) {
        addWorkerStats(threadQ[tidx]->getName().c_str(), threadQ[tidx],
                     cookie, add_stat);
        showJobLog("log", threadQ[tidx]->getName().c_str(),
                   threadQ[tidx]->getLog(), cookie, add_stat);
        showJobLog("slow", threadQ[tidx]->getName().c_str(),
                   threadQ[tidx]->getSlowLog(), cookie, add_stat);
    }
    ObjectRegistry::onSwitchThread(epe);
}

void ExecutorPool::doTasksStat(EventuallyPersistentEngine* engine,
                               const void* cookie,
                               ADD_STAT add_stat) {
    if (engine->getEpStats().isShutdown) {
        return;
    }

    EventuallyPersistentEngine* epe =
            ObjectRegistry::onSwitchThread(NULL, true);

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

    unique_cJSON_ptr list(cJSON_CreateArray());

    for (auto& pair : taskLocatorCopy) {
        size_t tid = pair.first;
        ExTask& task = pair.second.first;

        unique_cJSON_ptr obj(cJSON_CreateObject());

        cJSON_AddNumberToObject(obj.get(), "tid", tid);
        cJSON_AddStringToObject(
                obj.get(), "state", to_string(task->getState()).c_str());
        cJSON_AddStringToObject(
                obj.get(), "name", GlobalTask::getTaskName(task->getTaskId()));
        cJSON_AddStringToObject(
                obj.get(),
                "this",
                cb::to_hex(reinterpret_cast<uint64_t>(task.get())).c_str());
        cJSON_AddStringToObject(
                obj.get(), "bucket", task->getTaskable().getName().c_str());
        cJSON_AddStringToObject(
                obj.get(), "description", task->getDescription().data());
        cJSON_AddNumberToObject(
                obj.get(), "priority", task->getQueuePriority());
        cJSON_AddNumberToObject(obj.get(),
                                "waketime_ns",
                                task->getWaketime().time_since_epoch().count());
        cJSON_AddNumberToObject(
                obj.get(), "total_runtime_ns", task->getTotalRuntime().count());
        cJSON_AddNumberToObject(
                obj.get(),
                "last_starttime_ns",
                to_ns_since_epoch(task->getLastStartTime()).count());
        cJSON_AddNumberToObject(obj.get(),
                                "previous_runtime_ns",
                                task->getPrevRuntime().count());
        cJSON_AddNumberToObject(
                obj.get(),
                "num_runs",
                engine->getEpStats()
                        .taskRuntimeHisto[static_cast<int>(task->getTaskId())]
                        .total());
        cJSON_AddStringToObject(
                obj.get(),
                "type",
                TaskQueue::taskType2Str(
                        GlobalTask::getTaskType(task->getTaskId()))
                        .c_str());

        cJSON_AddItemToArray(list.get(), obj.release());
    }

    checked_snprintf(statname, sizeof(statname), "%s:tasks", prefix);
    add_casted_stat(statname, to_string(list, false), add_stat, cookie);

    checked_snprintf(statname, sizeof(statname), "%s:cur_time", prefix);
    add_casted_stat(statname,
                    to_ns_since_epoch(ProcessClock::now()).count(),
                    add_stat,
                    cookie);

    checked_snprintf(statname, sizeof(statname), "%s:uptime_s", prefix);
    add_casted_stat(statname, ep_current_time(), add_stat, cookie);

    ObjectRegistry::onSwitchThread(epe);
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
