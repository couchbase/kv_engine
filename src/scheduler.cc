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

#include "common.hh"
#include "ep_engine.h"
#include "scheduler.h"
#include "locks.hh"
#include "workload.h"
#include "statwriter.hh"

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

void ExecutorThread::moveReadyTasks(const struct timeval &tv) {
    if (!readyQueue.empty()) {
        return;
    }

    std::queue<ExTask> notReady;
    while (!futureQueue.empty()) {
        const ExTask &tid = futureQueue.top();
        if (less_tv(tid->waketime, tv)) {
            readyQueue.push(tid);
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

    while(!notReady.empty()) {
        const ExTask &tid = notReady.front();
        if (less_tv(tid->waketime, tv)) {
            readyQueue.push(tid);
        } else {
            futureQueue.push(tid);
        }
        notReady.pop();
    }
}

ExTask ExecutorThread::nextTask() {
    assert (!empty());
    return readyQueue.empty() ? futureQueue.top() : readyQueue.top();
}

void ExecutorThread::popNext() {
    assert (!empty());
    readyQueue.empty() ? futureQueue.pop() : readyQueue.pop();
}

void ExecutorThread::start() {
    assert(state == EXECUTOR_CREATING);
    if(pthread_create(&thread, NULL, launch_executor_thread, this) != 0) {
        std::stringstream ss;
        ss << name.c_str() << ": Initialization error!!!";
        throw std::runtime_error(ss.str().c_str());
    }
}

void ExecutorThread::stop() {
    LockHolder lh(mutex);
    if (state == EXECUTOR_SHUTDOWN || state == EXECUTOR_DEAD) {
        return;
    }
    LOG(EXTENSION_LOG_INFO, "%s: Stopping", name.c_str());
    state = EXECUTOR_SHUTDOWN;
    notify();
    lh.unlock();
    pthread_join(thread, NULL);
    LOG(EXTENSION_LOG_INFO, "%s: Stopped", name.c_str());
}

void ExecutorThread::run() {
    state = EXECUTOR_RUNNING;
    ObjectRegistry::onSwitchThread(engine);
    for (;;) {
        LockHolder lh(mutex);
        currentTask.reset();
        if (state != EXECUTOR_RUNNING) {
            break;
        }
        if (empty()) {
            state = EXECUTOR_WAITING;
            mutex.wait();
            if (state == EXECUTOR_WAITING) {
                state = EXECUTOR_RUNNING;
            }
        } else {
            struct timeval tv;
            gettimeofday(&tv, NULL);

            // Get any ready tasks out of the due queue.
            moveReadyTasks(tv);

            currentTask = nextTask();
            assert(currentTask);
            LockHolder tlh(currentTask->mutex);
            if (currentTask->state == TASK_DEAD) {
                popNext();
                continue;
            }
            tlh.unlock();

            if (less_tv(tv, currentTask->waketime)) {
                state = EXECUTOR_SLEEPING;
                mutex.wait(currentTask->waketime);
                if (state == EXECUTOR_SLEEPING) {
                    state = EXECUTOR_RUNNING;
                }
                continue;
            } else {
                popNext();
            }
            lh.unlock();

            taskStart = gethrtime();
            rel_time_t startReltime = ep_current_time();
            try {
                bool again = currentTask->run();
                if(again) {
                    reschedule(currentTask);
                } else if (!currentTask->isDaemonTask) {
                    manager->cancel(currentTask->taskId);
                } else {
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
            TaskLogEntry tle(currentTask->getDescription(), runtime, startReltime);
            tasklog.add(tle);
            if (runtime > (hrtime_t)currentTask->maxExpectedDuration()) {
                slowjobs.add(tle);
            }
        }
    }
    state = EXECUTOR_DEAD;
}

void ExecutorThread::schedule(ExTask &task) {
    if (state == EXECUTOR_SHUTDOWN || state == EXECUTOR_DEAD) {
        return;
    }

    LockHolder lh(mutex);
    readyQueue.push(task);
    notify();
    LOG(EXTENSION_LOG_DEBUG, "%s: Schedule a task \"%s\"", name.c_str(),
        task->getDescription().c_str());
}

void ExecutorThread::reschedule(ExTask &task) {
    LOG(EXTENSION_LOG_DEBUG, "%s: Reschedule a task \"%s\"", name.c_str(),
        task->getDescription().c_str());
    LockHolder lh(mutex);
    futureQueue.push(task);
}

void ExecutorThread::wake(ExTask &task) {
    LockHolder lh(mutex);
    LOG(EXTENSION_LOG_DEBUG, "%s: Wake a task \"%s\"", name.c_str(),
        task->getDescription().c_str());
    task->snooze(0, false);
    hasWokenTask = true;
    notify();
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

bool ExecutorPool::cancel(size_t taskId) {
    LockHolder lh(mutex);
    std::map<size_t, lookupId>::iterator itr = taskLocator.find(taskId);
    if (itr == taskLocator.end()) {
        return false;
    }

    itr->second.first->cancel();
    taskLocator.erase(itr);
    return true;
}

bool ExecutorPool::wake(size_t taskId) {
    LockHolder lh(mutex);
    std::map<size_t, lookupId>::iterator itr = taskLocator.find(taskId);
    if (itr != taskLocator.end()) {
        itr->second.second->wake(itr->second.first);
        return true;
    }
    return false;
}

bool ExecutorPool::snooze(size_t taskId, double tosleep) {
    LockHolder lh(mutex);
    std::map<size_t, lookupId>::iterator itr = taskLocator.find(taskId);
    if (itr != taskLocator.end()) {
        itr->second.first->snooze(tosleep, false);
        return true;
    }
    return false;
}

size_t ExecutorPool::schedule(ExTask task, int tidx) {
    LockHolder lh(mutex);
    if (bucketRegistry.find(task->getEngine()) == bucketRegistry.end()) {
        LOG(EXTENSION_LOG_WARNING, "Trying to schedule task for unregistered "
            "bucket %s", task->getEngine()->getName());
        return task->getId();
    }

    threadQ &threads = bucketRegistry[task->getEngine()];
    threads[tidx]->schedule(task);
    lookupId loc(task, threads[tidx]);
    taskLocator[task->getId()] = loc;
    return task->getId();
}

void ExecutorPool::registerBucket(EventuallyPersistentEngine *engine) {
    LockHolder lh(mutex);
    if(bucketRegistry.find(engine) == bucketRegistry.end()) {
        startWorkers(engine);
    } else {
        LOG(EXTENSION_LOG_WARNING, "Bucket %s is trying to re-register itself",
            engine->getName());
    }
}

void ExecutorPool::unregisterBucket(EventuallyPersistentEngine *engine) {
    LockHolder lh(mutex);
    std::map<EventuallyPersistentEngine*, threadQ>::iterator itr =
        bucketRegistry.find(engine);
    if (itr == bucketRegistry.end()) {
        return;
    }

    threadQ threads = itr->second;
    bucketRegistry.erase(itr);
    lh.unlock();

    for (int tidx = 0; tidx < threads.size(); ++tidx) {
        LOG(EXTENSION_LOG_INFO,
            "Waiting for thread[%d] to finish in bucket: %s", tidx,
            engine->getName());
        threads[tidx]->stop();
        delete threads[tidx];
    }
}

bool ExecutorPool::startWorkers(EventuallyPersistentEngine *engine) {
    if (bucketRegistry.find(engine) == bucketRegistry.end()) {
        WorkLoadPolicy &workload = engine->getWorkLoadPolicy();
        int numThreads = workload.calculateNumReaders() +
                         workload.calculateNumWriters();
        threadQ threads;
        threads.reserve(numThreads);
        for (int tidx = 0; tidx < numThreads; ++tidx) {
            std::stringstream ss;
            ss << "iomanager_worker_" << tidx;
            threads.push_back(new ExecutorThread(this, engine, ss.str()));
            threads.back()->start();
        }
        bucketRegistry[engine] = threads;
        return true;
    } else {
        LOG(EXTENSION_LOG_WARNING,
                "Warning: cannot add more worker threads during run time");
        return false;
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

static void addWorkerStats(const char *prefix, threadQ threads, size_t t,
                           const void *cookie, ADD_STAT add_stat) {
    char statname[80] = {0};
    snprintf(statname, sizeof(statname), "%s:state", prefix);
    add_casted_stat(statname, threads[t]->getStateName().c_str(),
            add_stat, cookie);
    snprintf(statname, sizeof(statname), "%s:task", prefix);
    add_casted_stat(statname, threads[t]->getTaskName().c_str(),
            add_stat, cookie);

    if (strcmp(threads[t]->getStateName().c_str(), "running") == 0) {
        snprintf(statname, sizeof(statname), "%s:runtime", prefix);
        add_casted_stat(statname,
                (gethrtime() - threads[t]->getTaskStart()) / 1000,
                add_stat, cookie);
    }

    showJobLog("log", prefix, threads[t]->getLog(), cookie, add_stat);
    showJobLog("slow", prefix, threads[t]->getSlowLog(), cookie,
               add_stat);
}

void ExecutorPool::doWorkerStat(EventuallyPersistentEngine *engine,
                               const void *cookie, ADD_STAT add_stat) {
    if (engine->getEpStats().shutdown.isShutdown == true) {
        return;
    }
    std::map<EventuallyPersistentEngine*, threadQ>::iterator itr =
            bucketRegistry.find(engine);
    if (itr == bucketRegistry.end()) {
        return;
    }

    threadQ threads = itr->second;
    for (size_t tidx = 0; tidx < threads.size(); ++tidx) {
        addWorkerStats(threads[tidx]->getName().c_str(), threads, tidx,
                     cookie, add_stat);
    }
}
