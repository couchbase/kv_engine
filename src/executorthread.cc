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
#include "executorpool.h"
#include "executorthread.h"
#include "taskqueue.h"
#include "ep_engine.h"

AtomicValue<size_t> GlobalTask::task_id_counter(1);

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
    std::string thread_name("mc:" + getName());
    // Only permitted 15 characters of name; therefore abbreviate thread names.
    std::string worker("_worker");
    std::string::size_type pos = thread_name.find(worker);
    if (pos != std::string::npos) {
        thread_name.replace(pos, worker.size(), "");
    }
    thread_name.resize(15);
    if (cb_create_named_thread(&thread, launch_executor_thread, this, 0,
                               thread_name.c_str()) != 0) {
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
                // release capacity back to TaskQueue
                manager->doneWork(curTaskType);
                manager->cancel(currentTask->taskId, true);
                continue;
            }

            // Measure scheduling overhead as difference between the time
            // that the task wanted to wake up and the current time
            gettimeofday(&now, NULL);
            struct timeval woketime = currentTask->waketime;
            uint64_t diffsec = now.tv_sec > woketime.tv_sec ?
                               now.tv_sec - woketime.tv_sec : 0;
            uint64_t diffusec = now.tv_usec > woketime.tv_usec ?
                                now.tv_usec - woketime.tv_usec : 0;

            engine->getEpStore()->logQTime(currentTask->getTypeId(),
                                   diffsec*1000000 + diffusec);

            taskStart = gethrtime();
            rel_time_t startReltime = ep_current_time();
            try {
                LOG(EXTENSION_LOG_DEBUG,
                    "%s: Run task \"%s\" id %d waketime %d",
                getName().c_str(), currentTask->getDescription().c_str(),
                currentTask->getId(), currentTask->waketime.tv_sec);

                // Now Run the Task ....
                currentTask->setState(TASK_RUNNING, TASK_SNOOZED);
                bool again = currentTask->run();

                // Task done, log it ...
                hrtime_t runtime((gethrtime() - taskStart) / 1000);
                engine->getEpStore()->logRunTime(currentTask->getTypeId(),
                                               runtime);
                ObjectRegistry::onSwitchThread(NULL);
                addLogEntry(engine->getName() + currentTask->getDescription(),
                        q->getQueueType(), runtime, startReltime,
                        (runtime >
                         (hrtime_t)currentTask->maxExpectedDuration()));
                ObjectRegistry::onSwitchThread(engine);
                // Check if task is run once or needs to be rescheduled..
                if (!again || currentTask->isdead()) {
                    // release capacity back to TaskQueue
                    manager->doneWork(curTaskType);
                    manager->cancel(currentTask->taskId, true);
                } else {
                    struct timeval timetowake;
                    // if a task has not set snooze, update its waketime to now
                    // before rescheduling for more accurate timing histograms
                    if (less_eq_tv(currentTask->waketime, now)) {
                        currentTask->waketime = now;
                    }
                    // release capacity back to TaskQueue ..
                    manager->doneWork(curTaskType);
                    timetowake = q->reschedule(currentTask, curTaskType);
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
        }
    }
    state = EXECUTOR_DEAD;
}

void ExecutorThread::addLogEntry(const std::string &desc,
                                 const task_type_t taskType,
                                 const hrtime_t runtime,
                                 rel_time_t t, bool isSlowJob) {
    LockHolder lh(logMutex);
    TaskLogEntry tle(desc, taskType, runtime, t);
    if (isSlowJob) {
        slowjobs.add(tle);
    } else {
        tasklog.add(tle);
    }
}

const std::string ExecutorThread::getStateName() {
    switch (state.load()) {
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
