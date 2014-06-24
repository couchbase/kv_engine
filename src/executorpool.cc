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

#include <queue>

#include "statwriter.h"
#include "taskqueue.h"
#include "executorpool.h"
#include "executorthread.h"

Mutex ExecutorPool::initGuard;
ExecutorPool *ExecutorPool::instance = NULL;

static const size_t EP_MIN_NUM_IO_THREADS = 4;

size_t ExecutorPool::getNumCPU(void) {
    size_t numCPU;
#ifdef WIN32
    SYSTEM_INFO sysinfo;
    GetSystemInfo(&sysinfo);
    numCPU = (size_t)sysinfo.dwNumberOfProcessors;
#else
    numCPU = (size_t)sysconf(_SC_NPROCESSORS_ONLN);
#endif

    return (numCPU < 256) ? numCPU : 0;
}

size_t ExecutorPool::getNumNonIO(void) {
    // ceil of 10 % of total threads
    size_t count = maxGlobalThreads / 10;
    return (!count || maxGlobalThreads % 10) ? count + 1 : count;
}

size_t ExecutorPool::getNumAuxIO(void) {
    // ceil of 10 % of total threads
    size_t count = maxGlobalThreads / 10;
    return (!count || maxGlobalThreads % 10) ? count + 1 : count;
}

size_t ExecutorPool::getNumWriters(void) {
    // floor of half of what remains after nonIO and auxIO threads are taken
    size_t count = maxGlobalThreads - getNumAuxIO() - getNumNonIO();
    count = count >> 1;
    return count ? count : 1;
}

size_t ExecutorPool::getNumReaders(void) {
    // what remains after writers, nonIO and auxIO threads are taken
    return(maxGlobalThreads - getNumWriters() - getNumAuxIO() - getNumNonIO());
}

ExecutorPool *ExecutorPool::get(void) {
    if (!instance) {
        LockHolder lh(initGuard);
        if (!instance) {
            Configuration &config =
                ObjectRegistry::getCurrentEngine()->getConfiguration();
            instance = new ExecutorPool(config.getMaxThreads(),
                                        NUM_TASK_GROUPS);
        }
    }
    return instance;
}

ExecutorPool::ExecutorPool(size_t maxThreads, size_t nTaskSets) :
                  numTaskSets(nTaskSets), numReadyTasks(0),
                  isHiPrioQset(false), isLowPrioQset(false), numBuckets(0) {
    size_t numCPU = getNumCPU();
    size_t numThreads = (size_t)((numCPU * 3)/4);
    numThreads = (numThreads < EP_MIN_NUM_IO_THREADS) ?
                        EP_MIN_NUM_IO_THREADS : numThreads;
    maxGlobalThreads = maxThreads ? maxThreads : numThreads;
    curSleepers = (uint16_t *)calloc(nTaskSets, sizeof(uint16_t));
    curWorkers  = (uint16_t *)calloc(nTaskSets, sizeof(uint16_t));
    maxWorkers  = (uint16_t *)malloc(nTaskSets*sizeof(uint16_t));
    for (size_t i = 0; i < nTaskSets; i++) {
        maxWorkers[i] = maxGlobalThreads;
    }
}

ExecutorPool::~ExecutorPool(void) {
    free(curSleepers);
    free(curWorkers);
    free(maxWorkers);
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

TaskQueue *ExecutorPool::nextTask(ExecutorThread &t, uint8_t tick) {
    if (!tick) {
        return NULL;
    }

    struct  timeval    now;
    gettimeofday(&now, NULL);
    int idx = t.startIndex;

    for (; !(tick % LOW_PRIORITY_FREQ); idx = (idx + 1) % numTaskSets) {
        if (isLowPrioQset &&
             lpTaskQ[idx]->fetchNextTask(t.currentTask, t.waketime,
                                         t.curTaskType, now)) {
            return lpTaskQ[idx];
        } else if (isHiPrioQset &&
             hpTaskQ[idx]->fetchNextTask(t.currentTask, t.waketime,
                                         t.curTaskType, now)) {
            return hpTaskQ[idx];
        } else if ((idx + 1) % numTaskSets == t.startIndex) {
            if (!trySleep(t, now)) { // as all queues checked & got no task
                return NULL; // executor is shutting down..
            }
        }
    }

    for (;; idx = (idx + 1) % numTaskSets) {
        if (isHiPrioQset &&
             hpTaskQ[idx]->fetchNextTask(t.currentTask, t.waketime,
                                         t.curTaskType, now)) {
            return hpTaskQ[idx];
        } else if (isLowPrioQset &&
             lpTaskQ[idx]->fetchNextTask(t.currentTask, t.waketime,
                                         t.curTaskType, now)) {
            return lpTaskQ[idx];
        } else if ((idx + 1) % numTaskSets == t.startIndex) {
            if (!trySleep(t, now)) { // as all queues checked & got no task
                return NULL; // executor is shutting down..
            }
        }
    }
    return NULL;
}

bool ExecutorPool::trySleep(ExecutorThread &t, struct timeval &now) {
    if (!numReadyTasks && less_tv(now, t.waketime)) {
        if (t.state == EXECUTOR_RUNNING) {
            t.state = EXECUTOR_SLEEPING;
        } else {
            LOG(EXTENSION_LOG_DEBUG, "%s: shutting down %d tasks ready",
                    t.getName().c_str(), numReadyTasks.load());
            return false;
        }

        LockHolder lh(mutex);
        LOG(EXTENSION_LOG_DEBUG, "%s: to sleep for %d s", t.getName().c_str(),
                (t.waketime.tv_sec - now.tv_sec));
        // zzz ....
        curSleepers[t.startIndex]++;
        numSleepers++;

        if (curSleepers[t.startIndex] // let only 1 thread per TaskSet nap
            || is_max_tv(t.waketime)) { // other threads sleep (saves CPU)
            advance_tv(now, MIN_SLEEP_TIME); // don't miss posts,
            mutex.wait(now); // timed sleeps are the safe way to go
        } else {
            mutex.wait(t.waketime);
        }

        //... got up
        numSleepers--;
        curSleepers[t.startIndex]--;
        lh.unlock();
        if (t.state == EXECUTOR_SLEEPING) {
            t.state = EXECUTOR_RUNNING;
        } else {
            LOG(EXTENSION_LOG_DEBUG, "%s: shutting down %d tasks ready",
                    t.getName().c_str(), numReadyTasks.load());
            return false;
        }

        gettimeofday(&now, NULL);
        LOG(EXTENSION_LOG_DEBUG, "%s: woke up %d tasks ready",
        t.getName().c_str(), numReadyTasks.load());
    }
    set_max_tv(t.waketime);
    return true;
}


void ExecutorPool::notifyOne(void) {
    LockHolder lh(mutex);
    mutex.notifyOne();
}

void ExecutorPool::notifyAll(void) {
    LockHolder lh(mutex);
    mutex.notify();
}

void ExecutorPool::addWork(size_t newWork) {
    numReadyTasks.fetch_add(newWork);
}

void ExecutorPool::wakeSleepers(size_t newWork) {
    if (!newWork) {
        return;
    }

    LockHolder lh(mutex);
    if (numSleepers) {
        if (newWork < numSleepers) {
            for (size_t i = 0; i < newWork; i++) {
                mutex.notifyOne(); // cond_signal
            }
        } else {
            mutex.notify(); // cond_broadcast
        }
    }
}

void ExecutorPool::moreWork(size_t newWork) {
    addWork(newWork);
    wakeSleepers(newWork);
}

void ExecutorPool::lessWork(void) {
    cb_assert(numReadyTasks.load());
    numReadyTasks--;
}

void ExecutorPool::doneWork(task_type_t &curTaskType) {
    if (curTaskType != NO_TASK_TYPE) {
        if (maxWorkers[curTaskType] != threadQ.size()) { // singleton constants
            // Record that a thread is done working on a particular queue type
            LockHolder lh(mutex);
            LOG(EXTENSION_LOG_DEBUG, "Done with Task Type %d capacity = %d",
                    curTaskType, curWorkers[curTaskType]);
            curWorkers[curTaskType]--;
            curTaskType = NO_TASK_TYPE;
        }
    }
}

task_type_t ExecutorPool::tryNewWork(task_type_t newTaskType) {
    task_type_t ret = newTaskType;
    if (maxWorkers[newTaskType] != threadQ.size()) { // dirty read of constants
        LockHolder lh(mutex);
        // Test if a thread can take up task from the target Queue type
        if (curWorkers[newTaskType] + 1 <= maxWorkers[newTaskType]) {
            curWorkers[newTaskType]++;
            LOG(EXTENSION_LOG_DEBUG,
                    "Taking up work in task type %d capacity = %d, max=%d",
                    newTaskType, curWorkers[newTaskType],
                    maxWorkers[newTaskType]);
        } else {
            LOG(EXTENSION_LOG_DEBUG, "Limiting from taking up work in task "
                    "type %d capacity = %d, max = %d", newTaskType,
                    curWorkers[newTaskType], maxWorkers[newTaskType]);
            ret = NO_TASK_TYPE;
        }
    }

    return ret;
}

bool ExecutorPool::cancel(size_t taskId, bool eraseTask) {
    LockHolder lh(tMutex);
    std::map<size_t, TaskQpair>::iterator itr = taskLocator.find(taskId);
    if (itr == taskLocator.end()) {
        LOG(EXTENSION_LOG_DEBUG, "Task id %d not found");
        return false;
    }

    ExTask task = itr->second.first;
    LOG(EXTENSION_LOG_DEBUG, "Cancel task %s id %d on bucket %s %s",
            task->getDescription().c_str(), task->getId(),
            task->getEngine()->getName(), eraseTask ? "final erase" : "!");

    task->cancel(); // must be idempotent, just set state to dead

    if (eraseTask) { // only internal threads can erase tasks
        cb_assert(task->isdead());
        taskLocator.erase(itr);
        tMutex.notify();
    } else { // wake up the task from the TaskQ so a thread can safely erase it
             // otherwise we may race with unregisterBucket where a unlocated
             // task runs in spite of its bucket getting unregistered
        itr->second.second->wake(task);
    }
    return true;
}

bool ExecutorPool::wake(size_t taskId) {
    LockHolder lh(tMutex);
    std::map<size_t, TaskQpair>::iterator itr = taskLocator.find(taskId);
    if (itr != taskLocator.end()) {
        itr->second.second->wake(itr->second.first);
        return true;
    }
    return false;
}

bool ExecutorPool::snooze(size_t taskId, double tosleep) {
    LockHolder lh(tMutex);
    std::map<size_t, TaskQpair>::iterator itr = taskLocator.find(taskId);
    if (itr != taskLocator.end()) {
        itr->second.first->snooze(tosleep);
        return true;
    }
    return false;
}

TaskQueue* ExecutorPool::getTaskQueue(EventuallyPersistentEngine *e,
                                      task_type_t qidx) {
    TaskQueue         *q             = NULL;
    size_t            curNumThreads  = 0;
    bucket_priority_t bucketPriority = e->getWorkloadPriority();

    cb_assert(0 <= (int)qidx && (size_t)qidx < numTaskSets);

    curNumThreads = threadQ.size();

    if (!bucketPriority) {
        LOG(EXTENSION_LOG_WARNING, "Trying to schedule task for unregistered "
            "bucket %s", e->getName());
        return q;
    }

    if (curNumThreads < maxGlobalThreads) {
        if (isHiPrioQset) {
            q = hpTaskQ[qidx];
        } else if (isLowPrioQset) {
            q = lpTaskQ[qidx];
        }
    } else { // Max capacity Mode scheduling ...
        if (bucketPriority == LOW_BUCKET_PRIORITY) {
            cb_assert(lpTaskQ.size() == numTaskSets);
            q = lpTaskQ[qidx];
        } else {
            cb_assert(hpTaskQ.size() == numTaskSets);
            q = hpTaskQ[qidx];
        }
    }
    return q;
}

size_t ExecutorPool::schedule(ExTask task, task_type_t qidx) {
    LockHolder lh(tMutex);
    TaskQueue *q = getTaskQueue(task->getEngine(), qidx);
    TaskQpair tqp(task, q);
    taskLocator[task->getId()] = tqp;

    q->schedule(task);

    return task->getId();
}

void ExecutorPool::registerBucket(EventuallyPersistentEngine *engine) {
    TaskQ *taskQ;
    bool *whichQset;
    const char *queueName;
    WorkLoadPolicy &workload = engine->getWorkLoadPolicy();
    bucket_priority_t priority = workload.getBucketPriority();

    if (priority < HIGH_BUCKET_PRIORITY) {
        engine->setWorkloadPriority(LOW_BUCKET_PRIORITY);
        taskQ = &lpTaskQ;
        whichQset = &isLowPrioQset;
        queueName = "LowPrioQ_";
        LOG(EXTENSION_LOG_WARNING, "Bucket %s registered with low priority",
            engine->getName());
    } else {
        engine->setWorkloadPriority(HIGH_BUCKET_PRIORITY);
        taskQ = &hpTaskQ;
        whichQset = &isHiPrioQset;
        queueName = "HiPrioQ_";
        LOG(EXTENSION_LOG_WARNING, "Bucket %s registered with high priority",
            engine->getName());
    }

    LockHolder lh(tMutex);

    if (!(*whichQset)) {
        taskQ->reserve(numTaskSets);
        for (size_t i = 0; i < numTaskSets; i++) {
            taskQ->push_back(new TaskQueue(this, (task_type_t)i, queueName));
        }
        *whichQset = true;
    }

    numBuckets++;

    startWorkers();
}

bool ExecutorPool::startWorkers(void) {
    if (threadQ.size()) {
        return false;
    }

    size_t numReaders = getNumReaders();
    size_t numWriters = getNumWriters();
    size_t numAuxIO   = getNumAuxIO();
    size_t numNonIO   = getNumNonIO();

    LOG(EXTENSION_LOG_WARNING,
            "Spawning %zu readers, %zu writers, %zu auxIO, %zu nonIO threads",
            numReaders, numWriters, numAuxIO, numNonIO);

    for (size_t tidx = 0; tidx < numReaders; ++tidx) {
        std::stringstream ss;
        ss << "reader_worker_" << tidx;

        threadQ.push_back(new ExecutorThread(this, READER_TASK_IDX, ss.str()));
        threadQ.back()->start();
    }
    for (size_t tidx = 0; tidx < numWriters; ++tidx) {
        std::stringstream ss;
        ss << "writer_worker_" << numReaders + tidx;

        threadQ.push_back(new ExecutorThread(this, WRITER_TASK_IDX, ss.str()));
        threadQ.back()->start();
    }
    for (size_t tidx = 0; tidx < numAuxIO; ++tidx) {
        std::stringstream ss;
        ss << "auxio_worker_" << numReaders + numWriters + tidx;

        threadQ.push_back(new ExecutorThread(this, AUXIO_TASK_IDX, ss.str()));
        threadQ.back()->start();
    }
    for (size_t tidx = 0; tidx < numNonIO; ++tidx) {
        std::stringstream ss;
        ss << "nonio_worker_" << numReaders + numWriters + numAuxIO + tidx;

        threadQ.push_back(new ExecutorThread(this, NONIO_TASK_IDX, ss.str()));
        threadQ.back()->start();
    }

    LockHolder lh(mutex);
    maxWorkers[AUXIO_TASK_IDX]  = numAuxIO;
    maxWorkers[NONIO_TASK_IDX]  = numNonIO;

    return true;
}

bool ExecutorPool::stopTaskGroup(EventuallyPersistentEngine *e,
                                 task_type_t taskType) {
    bool unfinishedTask;
    bool retVal = false;
    std::map<size_t, TaskQpair>::iterator itr;

    LockHolder lh(tMutex);
    LOG(EXTENSION_LOG_DEBUG, "Stopping %d type tasks in bucket %s", taskType,
            e->getName());
    do {
        ExTask task;
        unfinishedTask = false;
        for (itr = taskLocator.begin(); itr != taskLocator.end(); itr++) {
            task = itr->second.first;
            TaskQueue *q = itr->second.second;
            if (task->getEngine() == e &&
                (taskType == NO_TASK_TYPE || q->queueType == taskType)) {
                LOG(EXTENSION_LOG_DEBUG, "Stopping Task id %d %s ",
                        task->getId(), task->getDescription().c_str());
                if (!task->blockShutdown) {
                    task->cancel(); // Must be idempotent
                }
                q->wake(task);
                unfinishedTask = true;
                retVal = true;
            }
        }
        if (unfinishedTask) {
            struct timeval waktime;
            gettimeofday(&waktime, NULL);
            advance_tv(waktime, MIN_SLEEP_TIME);
            tMutex.wait(waktime); // Wait till task gets cancelled
        }
    } while (unfinishedTask);

    return retVal;
}

void ExecutorPool::unregisterBucket(EventuallyPersistentEngine *engine) {

    LOG(EXTENSION_LOG_DEBUG, "Unregistering bucket %s", engine->getName());

    stopTaskGroup(engine, NO_TASK_TYPE);

    LockHolder lh(tMutex);

    if (!(--numBuckets)) {
        assert (!taskLocator.size());
        LockHolder lm(mutex);
        for (size_t tidx = 0; tidx < threadQ.size(); ++tidx) {
            threadQ[tidx]->stop(false); // only set state to DEAD
        }

        mutex.notify();
        lm.unlock();

        for (size_t tidx = 0; tidx < threadQ.size(); ++tidx) {
            threadQ[tidx]->stop(/*wait for threads */);
            delete threadQ[tidx];
        }

        for (size_t i = 0; i < numTaskSets; i++) {
            curWorkers[i] = 0;
        }

        threadQ.clear();
        LOG(EXTENSION_LOG_DEBUG, "Last bucket has unregistered");
    }
}

void ExecutorPool::doTaskQStat(EventuallyPersistentEngine *engine,
                               const void *cookie, ADD_STAT add_stat) {
    if (engine->getEpStats().isShutdown) {
        return;
    }

    char statname[80] = {0};
    if (isHiPrioQset) {
        for (size_t i = 0; i < numTaskSets; i++) {
            snprintf(statname, sizeof(statname), "ep_workload:%s:InQsize",
                     hpTaskQ[i]->getName().c_str());
            add_casted_stat(statname, hpTaskQ[i]->futureQueue.size(), add_stat,
                            cookie);
            snprintf(statname, sizeof(statname), "ep_workload:%s:OutQsize",
                     hpTaskQ[i]->getName().c_str());
            add_casted_stat(statname, hpTaskQ[i]->readyQueue.size(), add_stat,
                            cookie);
            if (!hpTaskQ[i]->pendingQueue.empty()) {
                snprintf(statname, sizeof(statname), "ep_workload:%s:PendingQ",
                        hpTaskQ[i]->getName().c_str());
                add_casted_stat(statname, hpTaskQ[i]->pendingQueue.size(),
                                add_stat, cookie);
            }
        }
    }
    if (isLowPrioQset) {
        for (size_t i = 0; i < numTaskSets; i++) {
            snprintf(statname, sizeof(statname), "ep_workload:%s:InQsize",
                     lpTaskQ[i]->getName().c_str());
            add_casted_stat(statname, lpTaskQ[i]->futureQueue.size(), add_stat,
                            cookie);
            snprintf(statname, sizeof(statname), "ep_workload:%s:OutQsize",
                     lpTaskQ[i]->getName().c_str());
            add_casted_stat(statname, lpTaskQ[i]->readyQueue.size(), add_stat,
                            cookie);
            if (!lpTaskQ[i]->pendingQueue.empty()) {
                snprintf(statname, sizeof(statname), "ep_workload:%s:PendingQ",
                        lpTaskQ[i]->getName().c_str());
                add_casted_stat(statname, lpTaskQ[i]->pendingQueue.size(),
                                add_stat, cookie);
            }
        }
    }
}

static void showJobLog(const char *logname, const char *prefix,
                       std::vector<TaskLogEntry> log,
                       const void *cookie, ADD_STAT add_stat) {
    char statname[80] = {0};
    for (size_t i = 0;i < log.size(); ++i) {
        snprintf(statname, sizeof(statname), "%s:%s:%d:task", prefix,
                logname, static_cast<int>(i));
        add_casted_stat(statname, log[i].getName().c_str(), add_stat,
                        cookie);
        snprintf(statname, sizeof(statname), "%s:%s:%d:type", prefix,
                logname, static_cast<int>(i));
        add_casted_stat(statname,
                        TaskQueue::taskType2Str(log[i].getTaskType()).c_str(),
                        add_stat, cookie);
        snprintf(statname, sizeof(statname), "%s:%s:%d:starttime",
                prefix, logname, static_cast<int>(i));
        add_casted_stat(statname, log[i].getTimestamp(), add_stat,
                cookie);
        snprintf(statname, sizeof(statname), "%s:%s:%d:runtime",
                prefix, logname, static_cast<int>(i));
        add_casted_stat(statname, log[i].getDuration(), add_stat,
                cookie);
    }
}

static void addWorkerStats(const char *prefix, ExecutorThread *t,
                           const void *cookie, ADD_STAT add_stat) {
    char statname[80] = {0};
    snprintf(statname, sizeof(statname), "%s:state", prefix);
    add_casted_stat(statname, t->getStateName().c_str(), add_stat, cookie);
    snprintf(statname, sizeof(statname), "%s:task", prefix);
    add_casted_stat(statname, t->getTaskName().c_str(), add_stat, cookie);

    if (strcmp(t->getStateName().c_str(), "running") == 0) {
        snprintf(statname, sizeof(statname), "%s:runtime", prefix);
        add_casted_stat(statname,
                (gethrtime() - t->getTaskStart()) / 1000, add_stat, cookie);
    }
}

void ExecutorPool::doWorkerStat(EventuallyPersistentEngine *engine,
                               const void *cookie, ADD_STAT add_stat) {
    if (engine->getEpStats().isShutdown) {
        return;
    }

    //TODO: implement tracking per engine stats ..
    for (size_t tidx = 0; tidx < threadQ.size(); ++tidx) {
        addWorkerStats(threadQ[tidx]->getName().c_str(), threadQ[tidx],
                     cookie, add_stat);
        showJobLog("log", threadQ[tidx]->getName().c_str(),
                   threadQ[tidx]->getLog(), cookie, add_stat);
        showJobLog("slow", threadQ[tidx]->getName().c_str(),
                   threadQ[tidx]->getSlowLog(), cookie, add_stat);
    }
}
