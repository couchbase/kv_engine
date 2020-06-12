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

#include <chrono>
#include <queue>

#include "bucket_logger.h"
#include "executorpool.h"
#include "executorthread.h"
#include "globaltask.h"
#include "taskqueue.h"

#include <folly/Portability.h>
#include <folly/portability/SysResource.h>
#include <platform/timeutils.h>
#include <sstream>

extern "C" {
    static void launch_executor_thread(void *arg) {
        auto* executor = (CB3ExecutorThread*)arg;
        executor->run();
    }
}

CB3ExecutorThread::~CB3ExecutorThread() {
    EP_LOG_INFO("Executor killing {}", name);
}

void CB3ExecutorThread::start() {
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

    EP_LOG_DEBUG("{}: Started", name);
}

void CB3ExecutorThread::stop(bool wait) {
    if (!wait && (state == EXECUTOR_SHUTDOWN || state == EXECUTOR_DEAD)) {
        return;
    }

    state = EXECUTOR_SHUTDOWN;

    if (!wait) {
        EP_LOG_INFO("{}: Stopping", name);
        return;
    }
    cb_join_thread(thread);
    EP_LOG_INFO("{}: Stopped", name);
}

void CB3ExecutorThread::run() {
    EP_LOG_DEBUG("Thread {} running..", getName());

    // The run loop should not account to any bucket, only once inside a task
    // will accounting be back on. GlobalTask::execute will switch to the bucket
    // and CB3ExecutorPool::cancel will guard the task delete clause with the
    // BucketAllocationGuard to account task deletion to the bucket.
    NonBucketAllocationGuard guard;

    // Decrease the priority of Writer threads to lessen their impact on
    // other threads (esp front-end workers which should be prioritized ahead
    // of non-critical path Writer tasks (both Flusher and Compaction).
    // TODO: Investigate if it is worth increasing the priority of Flusher tasks
    // which _are_ on the critical path for front-end operations - i.e.
    // SyncWrites at level=persistMajority / persistActive.
    // This could be done by having two different priority thread pools (say
    // Low and High IO threads and putting critical path Reader (BGFetch) and
    // Writer (SyncWrite flushes) on the High IO thread pool; keeping
    // non-persist SyncWrites / normal mutations & compaction on the Low IO
    // pool.
#if defined(__linux__)
    // Only doing this for Linux at present:
    // - On Windows folly's getpriority() compatability function changes the
    //   priority of the entire process.
    // - On macOS setpriority(PRIO_PROCESS) affects the entire process (unlike
    //   Linux where it's only the current thread), hence calling setpriority()
    //   would be pointless.
    if (taskType == WRITER_TASK_IDX) {
        // Note Linux uses the range -20..19 (highest..lowest).
        const int lowestPriority = 19;
        if (setpriority(PRIO_PROCESS,
                        /*Current thread*/ 0,
                        lowestPriority)) {
            EP_LOG_WARN("Failed to set thread {} to background priority - {}",
                        getName(),
                        strerror(errno));
        } else {
            EP_LOG_INFO("Set thread {} to background priority", getName());
        }
    }
#endif

    priority = getpriority(PRIO_PROCESS, 0);

    for (uint8_t tick = 1;; tick++) {

        if (state != EXECUTOR_RUNNING) {
            break;
        }

        updateCurrentTime();
        if (TaskQueue *q = manager->nextTask(*this, tick)) {
            manager->startWork(taskType);

            if (currentTask->isdead()) {
                manager->doneWork(taskType);
                cancelCurrentTask(*manager);
                continue;
            }

            // Measure scheduling overhead as difference between the time
            // that the task wanted to wake up and the current time
            const std::chrono::steady_clock::time_point woketime =
                    currentTask->getWaketime();

            auto scheduleOverhead = getCurTime() - woketime;
            // scheduleOverhead can be a negative number if the task has been
            // woken up before we expected it too be. In this case this means
            // that we have no schedule overhead and thus need to set it too 0.
            if (scheduleOverhead.count() < 0) {
                scheduleOverhead = std::chrono::steady_clock::duration::zero();
            }

            currentTask->getTaskable().logQTime(currentTask->getTaskId(),
                                                scheduleOverhead);
            // MB-25822: It could be useful to have the exact datetime of long
            // schedule times, in the same way we have for long runtimes.
            // It is more difficult to estimate the expected schedule time than
            // the runtime for a task, because the schedule times depends on
            // things "external" to the task itself (e.g., how many tasks are
            // in queue in the same priority-group).
            // Also, the schedule time depends on the runtime of the previous
            // run. That means that for Read/Write/AuxIO tasks it is even more
            // difficult to predict because that do IO.
            // So, for now we log long schedule times only for NON_IO tasks,
            // which is the task type for the ConnManager and
            // ConnNotifierCallback tasks involved in MB-25822 and that we aim
            // to debug. We consider 1 second a sensible schedule overhead
            // limit for NON_IO tasks.
            if (GlobalTask::getTaskType(currentTask->getTaskId()) ==
                        task_type_t::NONIO_TASK_IDX &&
                scheduleOverhead > std::chrono::seconds(1)) {
                auto description = currentTask->getDescription();
                EP_LOG_WARN(
                        "Slow scheduling for NON_IO task '{}' on thread {}. "
                        "Schedule overhead: {}",
                        description,
                        getName(),
                        cb::time2text(scheduleOverhead));
            }
            updateTaskStart();

            const auto curTaskDescr = currentTask->getDescription();
            EP_LOG_TRACE("{}: Run task \"{}\" id {}",
                         getName(),
                         curTaskDescr,
                         currentTask->getId());

            // Now Run the Task ....
            currentTask->setState(TASK_RUNNING, TASK_SNOOZED);
            bool again = currentTask->execute();

            // Task done, log it ...
            const std::chrono::steady_clock::duration runtime(
                    std::chrono::steady_clock::now() - getTaskStart());
            currentTask->getTaskable().logRunTime(currentTask->getTaskId(),
                                                  runtime);
            currentTask->updateRuntime(runtime);

            // Check if exceeded expected duration; and if so log.
            // Note: This is done before we call onSwitchThread(NULL)
            // so the bucket name is included in the Log message.
            if (runtime > currentTask->maxExpectedDuration()) {
                auto description = currentTask->getDescription();
                EP_LOG_WARN("Slow runtime for '{}' on thread {}: {}",
                            description,
                            getName(),
                            cb::time2text(runtime));
            }

            // Check if task is run once or needs to be rescheduled..
            if (!again || currentTask->isdead()) {
                cancelCurrentTask(*manager);
            } else {
                // if a task has not set snooze, update its waketime to now
                // before rescheduling for more accurate timing histograms
                currentTask->updateWaketimeIfLessThan(getCurTime());

                // reschedule this task back into the future queue, based
                // on it's waketime.
                q->reschedule(currentTask);

                EP_LOG_TRACE(
                        "{}: Reschedule a task"
                        " \"{}\" id:{} waketime:{}",
                        name,
                        curTaskDescr,
                        currentTask->getId(),
                        to_ns_since_epoch(currentTask->getWaketime()).count());
                resetCurrentTask();
            }
            manager->doneWork(taskType);
        }
    }

    state = EXECUTOR_DEAD;
}

void CB3ExecutorThread::setCurrentTask(ExTask newTask) {
    LockHolder lh(currentTaskMutex);
    currentTask = newTask;
}

// MB-24394: reset currentTask, however we will perform the actual shared_ptr
// reset without the lock. This is because the task *can* re-enter the
// executorthread/pool code from it's destructor path, specifically if the task
// owns a VBucketPtr which is marked as "deferred-delete". Doing this std::move
// and lockless reset prevents a lock inversion.
void CB3ExecutorThread::resetCurrentTask() {
    ExTask resetThisObject;
    {
        LockHolder lh(currentTaskMutex);
        // move currentTask so we 'steal' the pointer and ensure currentTask
        // owns nothing.
        resetThisObject = std::move(currentTask);
    }
    resetThisObject.reset();
}

std::string CB3ExecutorThread::getTaskName() {
    LockHolder lh(currentTaskMutex);
    if (currentTask) {
        return currentTask->getDescription();
    } else {
        return "Not currently running any task";
    }
}

const std::string CB3ExecutorThread::getTaskableName() {
    LockHolder lh(currentTaskMutex);
    if (currentTask) {
        return currentTask->getTaskable().getName();
    } else {
        return std::string();
    }
}

const std::string CB3ExecutorThread::getStateName() {
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

void CB3ExecutorThread::cancelCurrentTask(CB3ExecutorPool& manager) {
    auto uid = currentTask->getId();
    resetCurrentTask();
    manager.cancel(uid, true);
}

task_type_t CB3ExecutorThread::getTaskType() const {
    return taskType;
}

int CB3ExecutorThread::getPriority() const {
    return priority;
}
