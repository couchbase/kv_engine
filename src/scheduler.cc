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

#include "common.h"
#include "ep_engine.h"
#include "locks.h"
#include "scheduler.h"
#include "statwriter.h"
#include "workload.h"
#include "taskqueue.h"

AtomicValue<size_t> GlobalTask::task_id_counter(1);
Mutex ExecutorPool::initGuard;
ExecutorPool *ExecutorPool::instance = NULL;

extern "C" {
    static void launch_executor_thread(void *arg) {
        ExecutorThread *executor = (ExecutorThread*) arg;
        try {
            executor->run();
        } catch (std::exception& e) {
            LOG(EXTENSION_LOG_WARNING, "%s: Caught an exception: %s\n",
                executor->getName().c_str(), e.what());
        } catch(...) {
            LOG(EXTENSION_LOG_WARNING, "%s: Caught a fatal exception\n",
                executor->getName().c_str());
        }
    }
}

void ExecutorThread::start() {
    assert(state == EXECUTOR_CREATING);
    if (cb_create_thread(&thread, launch_executor_thread, this, 0) != 0) {
        std::stringstream ss;
        ss << name.c_str() << ": Initialization error!!!";
        throw std::runtime_error(ss.str().c_str());
    }
}

void ExecutorThread::stop(bool wait) {
    if (!wait && (state == EXECUTOR_SHUTDOWN || state == EXECUTOR_DEAD)) {
        return;
    }
    state = EXECUTOR_SHUTDOWN;
    if (!wait) {
        LOG(EXTENSION_LOG_INFO, "%s: Stopping", name.c_str());
        return;
    }
    cb_join_thread(thread);
    LOG(EXTENSION_LOG_INFO, "%s: Stopped", name.c_str());
}

void ExecutorThread::run() {
    state = EXECUTOR_RUNNING;

    LOG(EXTENSION_LOG_DEBUG, "Thread %s running..", getName().c_str());

    for (uint8_t tick = 1;; tick++) {
        currentTask.reset();
        if (state != EXECUTOR_RUNNING) {
            break;
        }

        if (TaskQueue *q = manager->nextTask(*this, tick)) {
            EventuallyPersistentEngine *engine = currentTask->getEngine();
            ObjectRegistry::onSwitchThread(engine);
            if (currentTask->isdead()) {
                manager->cancel(currentTask->taskId, true);
                continue;
            }
            taskStart = gethrtime();
            rel_time_t startReltime = ep_current_time();
            try {
                LOG(EXTENSION_LOG_DEBUG,
                    "%s: Run task \"%s\" id %d waketime %d",
                getName().c_str(), currentTask->getDescription().c_str(),
                currentTask->getId(), currentTask->waketime.tv_sec);

                // Now Run the Task ....
                bool again = currentTask->run();

                // Task done ...
                if (!again || currentTask->isdead()) {
                    manager->cancel(currentTask->taskId, true);
                } else {
                    struct timeval timetowake;
                    timetowake = q->reschedule(currentTask);
                    // record min waketime ...
                    if (less_tv(timetowake, waketime)) {
                        waketime = timetowake;
                    }
                    LOG(EXTENSION_LOG_DEBUG,
                            "%s: Reschedule a task \"%s\" id %d[%d %d |%d]",
                            name.c_str(),
                            currentTask->getDescription().c_str(),
                            currentTask->getId(), timetowake.tv_sec,
                            currentTask->waketime.tv_sec,
                            waketime.tv_sec);
                }
            } catch (std::exception& e) {
                LOG(EXTENSION_LOG_WARNING,
                    "%s: Exception caught in task \"%s\": %s", name.c_str(),
                    currentTask->getDescription().c_str(), e.what());
            } catch(...) {
                LOG(EXTENSION_LOG_WARNING,
                    "%s: Fatal exception caught in task \"%s\"\n",
                    name.c_str(), currentTask->getDescription().c_str());
            }

            hrtime_t runtime((gethrtime() - taskStart) / 1000);
            TaskLogEntry tle(currentTask->getDescription(), runtime,
                             startReltime);
            tasklog.add(tle);
            if (runtime > (hrtime_t)currentTask->maxExpectedDuration()) {
                slowjobs.add(tle);
            }
        }
    }
    state = EXECUTOR_DEAD;
}

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
                  numTaskSets(nTaskSets), numReadyTasks(0), highWaterMark(0),
                  isHiPrioQset(false), isLowPrioQset(false), numBuckets(0) {
    maxGlobalThreads = maxThreads ? maxThreads : 2 * getNumCPU();
    curWorkers = (uint16_t *)calloc(nTaskSets, sizeof(uint16_t));
    maxWorkers = (uint16_t *)malloc(nTaskSets*sizeof(uint16_t));
    for (size_t i = 0; i < nTaskSets; i++) {
        maxWorkers[i] = maxGlobalThreads;
    }
}

ExecutorPool::~ExecutorPool(void) {
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
    size_t idx = t.startIndex;

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
    LockHolder lh(mutex);
    if (!numReadyTasks && less_tv(now, t.waketime)) {
        if (t.state == EXECUTOR_RUNNING) {
            t.state = EXECUTOR_SLEEPING;
        } else {
            LOG(EXTENSION_LOG_DEBUG, "%s: shutting down %d tasks ready",
                    t.getName().c_str(), numReadyTasks);
            return false;
        }

        LOG(EXTENSION_LOG_DEBUG, "%s: to sleep for %d s", t.getName().c_str(),
                (t.waketime.tv_sec - now.tv_sec));
        // zzz ....
        if (is_max_tv(t.waketime)) { // in absence of reliable posting
            advance_tv(now, MIN_SLEEP_TIME); // don't miss posts,
            mutex.wait(now); // timed sleeps are the safe way to go
        } else {
            mutex.wait(t.waketime);
        }

        // got up ..
        if (t.state == EXECUTOR_SLEEPING) {
            t.state = EXECUTOR_RUNNING;
        } else {
            LOG(EXTENSION_LOG_DEBUG, "%s: shutting down %d tasks ready",
                    t.getName().c_str(), numReadyTasks);
            return false;
        }

        gettimeofday(&now, NULL);
        LOG(EXTENSION_LOG_DEBUG, "%s: woke up %d tasks ready",
        t.getName().c_str(), numReadyTasks);
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

void ExecutorPool::moreWork(void) {
    LockHolder lh(mutex);
    numReadyTasks++;
    highWaterMark = (numReadyTasks > highWaterMark) ?
                     numReadyTasks : highWaterMark;

    mutex.notifyOne();
}

void ExecutorPool::lessWork(void) {
    assert(numReadyTasks);
    LockHolder lh(mutex);
    numReadyTasks--;
}

void ExecutorPool::doneWork(int &curTaskType) {
    LockHolder lh(mutex);
    // First record that a thread is done working on a particular queue type
    if (curTaskType != NO_TASK_TYPE) {
      LOG(EXTENSION_LOG_DEBUG, "Done with Task Type %d capacity = %d",
              curTaskType, curWorkers[curTaskType]);
      curWorkers[curTaskType]--;
    }
    curTaskType = NO_TASK_TYPE;
}

int ExecutorPool::tryNewWork(int newTaskType) {
    LockHolder lh(mutex);
    // Test if a thread can take up task from the target Queue type
    if (curWorkers[newTaskType] + 1 <= maxWorkers[newTaskType]) {
        curWorkers[newTaskType]++;
        LOG(EXTENSION_LOG_DEBUG,
                "Taking up work in task type %d capacity = %d",
                newTaskType, curWorkers[newTaskType]);
        return newTaskType;
    }

    LOG(EXTENSION_LOG_DEBUG, "Limiting from taking up work in task "
            "type %d capacity = %d", newTaskType, maxWorkers[newTaskType]);
    return NO_TASK_TYPE;
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
        assert(task->isdead());
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
        itr->second.first->snooze(tosleep, false);
        return true;
    }
    return false;
}

TaskQueue* ExecutorPool::getTaskQueue(EventuallyPersistentEngine *e,
                                      task_type_t qidx) {
    TaskQueue         *q             = NULL;
    size_t            curNumThreads  = 0;
    bucket_priority_t bucketPriority = e->getWorkloadPriority();

    assert(0 <= (int)qidx && (size_t)qidx < numTaskSets);

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
            assert(lpTaskQ.size() == numTaskSets);
            q = lpTaskQ[qidx];
        } else {
            assert(hpTaskQ.size() == numTaskSets);
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
        queueName = "Low Priority Bucket TaskQ";
        LOG(EXTENSION_LOG_WARNING, "Bucket %s registered with low priority",
            engine->getName());
    } else {
        engine->setWorkloadPriority(HIGH_BUCKET_PRIORITY);
        taskQ = &hpTaskQ;
        whichQset = &isHiPrioQset;
        queueName = "High Priority Bucket TaskQ";
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

const std::string ExecutorThread::getStateName() {
    switch (state) {
    case EXECUTOR_CREATING:
        return std::string("creating");
    case EXECUTOR_RUNNING:
        return std::string("running");
    case EXECUTOR_WAITING:
        return std::string("waiting");
    case EXECUTOR_SLEEPING:
        return std::string("sleeping");
    case EXECUTOR_SHUTDOWN:
        return std::string("shutdown");
    default:
        return std::string("dead");
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

    showJobLog("log", prefix, t->getLog(), cookie, add_stat);
    showJobLog("slow", prefix, t->getSlowLog(), cookie, add_stat);
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
    }
}
