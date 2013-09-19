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

Atomic<size_t> GlobalTask::task_id_counter = 1;

extern "C" {
    static void* launch_executor_thread(void* arg);
}

static void* launch_executor_thread(void *arg) {
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
    return NULL;
}

void ExecutorThread::start() {
    assert(state == EXECUTOR_CREATING);
    if (pthread_create(&thread, NULL, launch_executor_thread, this) != 0) {
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
    pthread_join(thread, NULL);
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
                LOG(EXTENSION_LOG_DEBUG, "%s: Run task \"%s\" id %d waketime %d",
                getName().c_str(), currentTask->getDescription().c_str(),
                currentTask->getId(), currentTask->waketime.tv_sec);

                // Now Run the Task ....
                bool again = currentTask->run();

                // Task done ...
                if (!again) {
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
                    "%s: Fatal exception caught in task \"%s\"\n", name.c_str(),
                    currentTask->getDescription().c_str());
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

void TaskQueue::pushReadyTask(ExTask &tid) {
    readyQueue.push(tid);
    manager->moreWork();
}

ExTask TaskQueue::popReadyTask(void) {
    ExTask t = readyQueue.top();
    readyQueue.pop();
    manager->lessWork();
    return t;
}

bool TaskQueue::fetchNextTask(ExTask &task, struct timeval &waketime,
                              struct timeval now) {
    bool gotTask = false;
    LockHolder lh(mutex);

    if (empty()) { return gotTask; }

    moveReadyTasks(now);

    if (!readyQueue.empty()) {
        task = popReadyTask();
        gotTask = true;
    }

    if (!futureQueue.empty() &&
        less_tv(futureQueue.top()->waketime, waketime)) {
        waketime = futureQueue.top()->waketime; // record earliest waketime
    }

    return gotTask;
}

void TaskQueue::moveReadyTasks(struct timeval tv) {
    if (!readyQueue.empty()) {
        return;
    }

    std::queue<ExTask> notReady;
    while (!futureQueue.empty()) {
        ExTask tid = futureQueue.top();
        if (less_tv(tid->waketime, tv)) {
            pushReadyTask(tid);
        } else {
            // If we have woken a task recently the future queue might be out
            // of order so we need to check each job.
            if (hasWokenTask) {
                notReady.push(tid);
            } else {
                return;
            }
        }
        futureQueue.pop();
    }
    hasWokenTask = false;

    while (!notReady.empty()) {
        ExTask tid = notReady.front();
        if (less_tv(tid->waketime, tv)) {
            pushReadyTask(tid);
        } else {
            futureQueue.push(tid);
        }
        notReady.pop();
    }
}

void TaskQueue::schedule(ExTask &task) {
    LockHolder lh(mutex);

    futureQueue.push(task);
    manager->notifyOne();

    LOG(EXTENSION_LOG_DEBUG, "%s: Schedule a task \"%s\" id %d",
            name.c_str(), task->getDescription().c_str(), task->getId());
}

struct timeval TaskQueue::reschedule(ExTask &task) {
    LockHolder lh(mutex);
    futureQueue.push(task);
    return futureQueue.top()->waketime;
}

void TaskQueue::wake(ExTask &task) {
    LockHolder lh(mutex);
    LOG(EXTENSION_LOG_DEBUG, "%s: Wake a task \"%s\" id %d", name.c_str(),
            task->getDescription().c_str(), task->getId());
    task->snooze(0, false);
    hasWokenTask = true;
    manager->notifyOne();
}

// To prevent starvation of medium and low priority queues, we define their
// polling frequencies as follows ...
#define MED_PRIORITY_FREQ 3 // 1 out of 3 times threads check med priority Q
#define LOW_PRIORITY_FREQ 5 // 1 out of 5 times threads check low priority Q

TaskQueue *ExecutorPool::nextTask(ExecutorThread &t, uint8_t tick) {
    if (!tick) {
        return NULL;
    }

    struct  timeval    now;
    gettimeofday(&now, NULL);
    size_t idx = t.startIndex;
    for (; !(tick % MED_PRIORITY_FREQ); idx = (idx + 1) % numTaskSets) {
        if (isMedPrioQset &&
            mpTaskQ[idx]->fetchNextTask(t.currentTask, t.waketime, now)) {
            return mpTaskQ[idx];
        } else if (isHiPrioQset &&
             hpTaskQ[idx]->fetchNextTask(t.currentTask, t.waketime, now)) {
            return hpTaskQ[idx];
        } else if (isLowPrioQset &&
             lpTaskQ[idx]->fetchNextTask(t.currentTask, t.waketime, now)) {
            return lpTaskQ[idx];
        } else if ((idx + 1) % numTaskSets == t.startIndex) {
            if (!trySleep(t, now)) { // as all queues checked & got no task
                return NULL; // executor is shutting down..
            }
        }
    }

    for (; !(tick % LOW_PRIORITY_FREQ); idx = (idx + 1) % numTaskSets) {
        if (isLowPrioQset &&
             lpTaskQ[idx]->fetchNextTask(t.currentTask, t.waketime, now)) {
            return lpTaskQ[idx];
        } else if (isHiPrioQset &&
             hpTaskQ[idx]->fetchNextTask(t.currentTask, t.waketime, now)) {
            return hpTaskQ[idx];
        } else if (isMedPrioQset &&
            mpTaskQ[idx]->fetchNextTask(t.currentTask, t.waketime, now)) {
            return mpTaskQ[idx];
        } else if ((idx + 1) % numTaskSets == t.startIndex) {
            if (!trySleep(t, now)) { // as all queues checked & got no task
                return NULL; // executor is shutting down..
            }
        }
    }

    for (;; idx = (idx + 1) % numTaskSets) {
        if (isHiPrioQset &&
             hpTaskQ[idx]->fetchNextTask(t.currentTask, t.waketime, now)) {
            return hpTaskQ[idx];
        } else if (isMedPrioQset &&
            mpTaskQ[idx]->fetchNextTask(t.currentTask, t.waketime, now)) {
            return mpTaskQ[idx];
        } else if (isLowPrioQset &&
             lpTaskQ[idx]->fetchNextTask(t.currentTask, t.waketime, now)) {
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
        if (is_max_tv(t.waketime)) {
            mutex.wait();
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

void ExecutorPool::moreWork(void) {
    LockHolder lh(mutex);
    numReadyTasks++;
    highWaterMark = (numReadyTasks > highWaterMark) ?
                     numReadyTasks : highWaterMark;

    if (numReadyTasks > threadQ.size()) {
        defaultQ = NULL;
    }

    mutex.notifyOne();
}

void ExecutorPool::lessWork(void) {
    assert(numReadyTasks);
    LockHolder lh(mutex);
    numReadyTasks--;
}

void ExecutorPool::setDefaultQ(bool reset) {
    if (!reset) {
        if (isHiPrioQset) {
            defaultQ = &hpTaskQ;
        } else if (isMedPrioQset) {
            defaultQ = &mpTaskQ;
        } else if (isLowPrioQset) {
            defaultQ = &lpTaskQ;
        } else {
            defaultQ = NULL;
        }
    } else {
        defaultQ = NULL;
    }
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

size_t ExecutorPool::schedule(ExTask task, int qidx) {
    TaskQueue         *q             = NULL;
    size_t            curNumThreads  = 0;
    bucket_priority_t bucketPriority = task->getEngine()->getWorkloadPriority();

    assert(0 <= qidx && qidx < numTaskSets);

    LockHolder lh(tMutex);
    curNumThreads = threadQ.size();

    if (!bucketPriority) {
        LOG(EXTENSION_LOG_WARNING, "Trying to schedule task for unregistered "
            "bucket %s", task->getEngine()->getName());
        return task->getId();
    }

    if (curNumThreads < maxIOThreads) {
        if (isHiPrioQset) {
            q = hpTaskQ[qidx];
        } else if (isMedPrioQset) {
            q = mpTaskQ[qidx];
        } else if (isLowPrioQset) {
            q = lpTaskQ[qidx];
        }
    } else { // Max capacity Mode scheduling ...
        if (bucketPriority == MEDIUM_BUCKET_PRIORITY) {
            assert(mpTaskQ.size() == numTaskSets);
            q = mpTaskQ[qidx];
        } else if (bucketPriority == LOW_BUCKET_PRIORITY) {
            assert(lpTaskQ.size() == numTaskSets);
            q = lpTaskQ[qidx];
        } else {
            assert(hpTaskQ.size() == numTaskSets);
            q = hpTaskQ[qidx];
        }
    }

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
    size_t numReaders = workload.calculateNumReaders();
    size_t numWriters = workload.calculateNumWriters();
    size_t numWorkers = numReaders + numWriters;

    //TODO: just use one queue until we hit maxThreads

    if (numWorkers < MEDIUM_BUCKET_PRIORITY) {
        engine->setWorkloadPriority(LOW_BUCKET_PRIORITY);
        taskQ = &lpTaskQ;
        whichQset = &isLowPrioQset;
        queueName = "Low Priority Bucket TaskQ";
        LOG(EXTENSION_LOG_WARNING, "Bucket %s registered with low priority",
            engine->getName());
    } else if (numWorkers < HIGH_BUCKET_PRIORITY) {
        engine->setWorkloadPriority(MEDIUM_BUCKET_PRIORITY);
        taskQ = &mpTaskQ;
        whichQset = &isMedPrioQset;
        queueName = "Medium Priority Bucket TaskQ";
        LOG(EXTENSION_LOG_WARNING, "Bucket %s registered with medium priority",
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
        for (int i = 0; i < numTaskSets; i++) {
            taskQ->push_back(new TaskQueue(this, queueName));
        }
        *whichQset = true;
    }

    numBuckets++;

    LOG(EXTENSION_LOG_DEBUG,
            "%s: numWorkers = %ld Max Threads = %d numTasks = %d",
            engine->getName(), numWorkers, maxIOThreads, taskQ->size());

    startWorkers(numReaders, numWriters);
}

bool ExecutorPool::startWorkers(size_t numReaders, size_t numWriters) {
    size_t numThreads = numReaders + numWriters;
    size_t curNumThreads = threadQ.size();
    if (curNumThreads == maxIOThreads) {
        LOG(EXTENSION_LOG_WARNING,
                "Warning: Max IO Thread limit %d reached!", maxIOThreads);
        return false;
    }

    if (curNumThreads + numThreads > maxIOThreads) {
        numThreads = maxIOThreads - curNumThreads;
        numWriters = numThreads >> 1; // floor of divide by 2 (favor readers)
        LOG(EXTENSION_LOG_WARNING,
                "Warning: Max IO Thread limit %d hit! Spawn only %d threads",
                maxIOThreads, numThreads);
    }

    for (int tidx = 0; tidx < numThreads; ++tidx) {
        std::stringstream ss;
        size_t startIdx = 1;
        if (numWriters) {
            startIdx = 0;
            numWriters--;
        }
        ss << "iomanager_worker_" << curNumThreads + tidx;

        threadQ.push_back(new ExecutorThread(this, startIdx, ss.str()));
        threadQ.back()->start();
    }
    return true;
}

void ExecutorPool::unregisterBucket(EventuallyPersistentEngine *engine) {
    bool unfinishedTask;
    std::map<size_t, TaskQpair>::iterator itr;

    LockHolder lh(tMutex);
    LOG(EXTENSION_LOG_DEBUG, "Unregistering bucket %s", engine->getName());
    do {
        unfinishedTask = false;
        for (itr = taskLocator.begin(); itr != taskLocator.end(); itr++) {
            ExTask task = itr->second.first;
            if (task->getEngine() == engine) {
                LOG(EXTENSION_LOG_DEBUG, "waiting for task id %d %s ",
                        task->getId(), task->getDescription().c_str());
                unfinishedTask = true;
                break;
            }
        }
        if (unfinishedTask) {
            tMutex.wait(); // Wait till task gets cancelled
        }
    } while(unfinishedTask);

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
    if (engine->getEpStats().shutdown.isShutdown == true) {
        return;
    }

    //TODO: implement tracking per engine stats ..
    for (size_t tidx = 0; tidx < threadQ.size(); ++tidx) {
        addWorkerStats(threadQ[tidx]->getName().c_str(), threadQ[tidx],
                     cookie, add_stat);
    }
}
