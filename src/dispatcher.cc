/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2010 Couchbase, Inc
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

#include "dispatcher.h"
#include "objectregistry.h"

extern "C" {
    static void launch_dispatcher_thread(void *arg) {
        Dispatcher *dispatcher = (Dispatcher*) arg;
        try {
            dispatcher->run();
        } catch (std::exception& e) {
            LOG(EXTENSION_LOG_WARNING, "%s: Caught an exception: %s\n",
                dispatcher->getName().c_str(), e.what());
        } catch(...) {
            LOG(EXTENSION_LOG_WARNING, "%s: Caught a fatal exception\n",
                dispatcher->getName().c_str());
        }
    }
}

/**
 * It simulates timegm to convert the given GMT time to number of seconds
 * since epoch but it uses C standard time functions.
 */
static time_t do_timegm(struct tm *tmv)
{
  time_t epoch = 0;
  time_t offset = mktime(gmtime(&epoch));
  time_t gmt = mktime(tmv);
  return difftime(gmt, offset);
}

void Task::snooze(const double secs, bool first) {
    LockHolder lh(mutex);
    gettimeofday(&waketime, NULL);
    size_t start = callback ? callback->startTime() : 24;
    // set scheduled task time for new task only
    if (first && (start == 0 || start <= 23) && secs >= 3600) {
        struct tm tim;
        struct timeval tmval = waketime;
        time_t seconds = tmval.tv_sec;
        tim = *(gmtime(&seconds));
        // change tm structure to the given start hour in GMT
        tim.tm_min = 0;
        tim.tm_sec = 0;
        if (tim.tm_hour >= (time_t)start) {
            tim.tm_hour = (time_t)start;
            tmval.tv_sec = do_timegm(&tim);
            // advance time until later than current time
            while (tmval.tv_sec < waketime.tv_sec) {
                advance_tv(tmval, secs);
            }
        } else if (tim.tm_hour < (time_t)start) {
            tim.tm_hour = start;
            tmval.tv_sec = do_timegm(&tim);
            // backtrack time until last time larger than current time
            time_t tsec;
            while ((tsec = tmval.tv_sec - (int)secs) > waketime.tv_sec) {
                tmval.tv_sec = tsec;
            }
        }
        waketime = tmval;
    } else {
        advance_tv(waketime, secs);
    }
}

void Dispatcher::start() {
    assert(state == dispatcher_running);

    if (cb_create_thread(&thread, launch_dispatcher_thread, this, 0) != 0) {
        std::stringstream ss;
        ss << getName().c_str() << ": Initialization error!!!";
        throw std::runtime_error(ss.str().c_str());
    }
}

TaskId Dispatcher::nextTask() {
    assert (!empty());
    return readyQueue.empty() ? futureQueue.top() : readyQueue.top();
}

void Dispatcher::popNext() {
    assert (!empty());
    readyQueue.empty() ? futureQueue.pop() : readyQueue.pop();
}

void Dispatcher::moveReadyTasks(const struct timeval &tv) {
    if (!readyQueue.empty()) {
        return;
    }

    std::queue<TaskId> notReady;
    while (!futureQueue.empty()) {
        const TaskId &tid = futureQueue.top();
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

    while(!notReady.empty()) {
        futureQueue.push(notReady.front());
        notReady.pop();
    }
    hasWokenTask = false;
}

void Dispatcher::run() {
    ObjectRegistry::onSwitchThread(&engine);
    LOG(EXTENSION_LOG_INFO, "%s: Starting", getName().c_str());
    for (;;) {
        LockHolder lh(mutex);
        // Having acquired the lock, verify our state and break out if
        // it's changed.
        if (state != dispatcher_running) {
            break;
        }

        if (empty()) {
            // Wait forever as long as the state didn't change while
            // we grabbed the lock.
            if (state == dispatcher_running) {
                noTask();
                mutex.wait();
            }
        } else {
            struct timeval tv;
            gettimeofday(&tv, NULL);

            // Get any ready tasks out of the due queue.
            moveReadyTasks(tv);

            TaskId task = nextTask();
            assert(task);
            LockHolder tlh(task->mutex);
            if (task->state == task_dead) {
                popNext();
                continue;
            }

            if (less_tv(tv, task->waketime)) {
                idleTask->setWaketime(task->waketime);
                idleTask->setDispatcherNotifications(notifications.get());
                task = static_cast<Task *>(idleTask.get());
                taskDesc = task->getName();
            } else {
                // Otherwise, do the normal thing.
                popNext();
                taskDesc = task->getName();
            }
            tlh.unlock();

            taskStart = gethrtime();
            lh.unlock();
            rel_time_t startReltime = ep_current_time();
            try {
                running_task = true;
                if(task->run(*this, task)) {
                    reschedule(task);
                }
            } catch (std::exception& e) {
                LOG(EXTENSION_LOG_WARNING,
                    "%s: Exception caught in task \"%s\": %s",
                    getName().c_str(), task->getName().c_str(), e.what());
            } catch(...) {
                LOG(EXTENSION_LOG_WARNING,
                    "%s: Fatal exception caught in task \"%s\"\n",
                    getName().c_str(), task->getName().c_str());
            }
            running_task = false;

            hrtime_t runtime((gethrtime() - taskStart) / 1000);
            JobLogEntry jle(taskDesc, runtime, startReltime);
            joblog.add(jle);
            if (runtime > task->maxExpectedDuration()) {
                slowjobs.add(jle);
            }
        }
    }

    completeNonDaemonTasks();
    state = dispatcher_stopped;
    notify();
    LOG(EXTENSION_LOG_INFO,"%s: Exited", getName().c_str());
}

void Dispatcher::stop(bool force) {
    LockHolder lh(mutex);
    if (state == dispatcher_stopped || state == dispatcher_stopping) {
        return;
    }
    forceTermination = force;
    LOG(EXTENSION_LOG_INFO, "%s: Stopping", getName().c_str());
    state = dispatcher_stopping;
    notify();
    lh.unlock();
    cb_join_thread(thread);
    LOG(EXTENSION_LOG_INFO, "%s: Stopped", getName().c_str());
}

void Dispatcher::schedule(shared_ptr<DispatcherCallback> callback,
                          TaskId *outtid,
                          const Priority &priority,
                          double sleeptime,
                          bool isDaemon,
                          bool mustComplete)
{
    // MB-4930 We might end up with a deadlock if some of the tasks try
    //         to reschedule new tasks during shutdown (while we're
    //         running completeNonDaemonTasks
    if (state == dispatcher_stopping || state == dispatcher_stopped) {
        return ;
    }

    LockHolder lh(mutex);
    TaskId task(new Task(callback, priority.getPriorityValue(), sleeptime,
                         isDaemon, mustComplete));
    if (outtid) {
        *outtid = task;
    }

    LOG(EXTENSION_LOG_DEBUG, "%s: Schedule a task \"%s\"",
        getName().c_str(), task->getName().c_str());

    futureQueue.push(task);
    notify();
}

void Dispatcher::wake(TaskId &task) {
    LockHolder lh(mutex);
    hasWokenTask = true;
    task->snooze(0);
    LOG(EXTENSION_LOG_DEBUG, "%s: Wake a task \"%s\"",
        getName().c_str(), task->getName().c_str());
    notify();
}

void Dispatcher::snooze(TaskId &t, double sleeptime) {
    LOG(EXTENSION_LOG_DEBUG, "%s: Snooze a task \"%s\"",
        getName().c_str(), t->getName().c_str());
    t->snooze(sleeptime);
}

void Dispatcher::cancel(TaskId &t) {
    LOG(EXTENSION_LOG_DEBUG, "%s: Cancel a task \"%s\"",
        getName().c_str(), t->getName().c_str());
    t->cancel();
}

void Dispatcher::reschedule(TaskId &task) {
    // If the task is already in the queue it'll get run twice
    LockHolder lh(mutex);
    LOG(EXTENSION_LOG_DEBUG, "%s: Reschedule a task \"%s\"",
        getName().c_str(), task->getName().c_str());
    futureQueue.push(task);
    notify();
}

void Dispatcher::completeNonDaemonTasks() {
    LockHolder lh(mutex);
    while (!empty()) {
        TaskId task = nextTask();
        popNext();
        assert(task);
        // Skip a daemon task
        if (task->isDaemonTask) {
            LOG(EXTENSION_LOG_INFO,
                "%s: Skipping daemon task \"%s\" during shutdown\n",
                getName().c_str(), task->getName().c_str());

            continue;
        }

        if (task->blockShutdown || !forceTermination) {
            LockHolder tlh(task->mutex);
            if (task->state == task_running) {
                tlh.unlock();
                LOG(EXTENSION_LOG_DEBUG,
                    "%s: Running task \"%s\" during shutdown",
                    getName().c_str(), task->getName().c_str());
                try {
                    while (task->run(*this, task)) {
                        LOG(EXTENSION_LOG_DEBUG,
                            "%s: Keep on running task \"%s\" during shutdown",
                            getName().c_str(), task->getName().c_str());
                    }
                } catch (std::exception& e) {
                    LOG(EXTENSION_LOG_WARNING,
                        "%s: Exception caught in task \"%s\" "
                        "during shutdown: \"%s\"",
                        getName().c_str(), task->getName().c_str(), e.what());
                } catch (...) {
                    LOG(EXTENSION_LOG_WARNING,
                        "%s: Fatal exception caught in task \"%s\" "
                        "during shutdown",
                        getName().c_str(), task->getName().c_str());
                }
                LOG(EXTENSION_LOG_DEBUG,
                    "%s: Task \"%s\" completed during shutdown",
                    getName().c_str(), task->getName().c_str());
            }
        } else {
            LOG(EXTENSION_LOG_INFO, "%s: Skipping task \"%s\" during shutdown",
                getName().c_str(), task->getName().c_str());
        }
    }

    LOG(EXTENSION_LOG_INFO,
        "%s: Completed all the non-daemon tasks as part of shutdown\n",
        getName().c_str());
}

bool IdleTask::run(Dispatcher &d, TaskId &) {
    LockHolder lh(d.mutex);
    if (d.notifications.get() == dnotifications) {
        d.mutex.wait(waketime);
    }
    return false;
}
