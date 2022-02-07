/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2013-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "cb3_executorthread.h"
#include "cb3_executorpool.h"
#include "cb3_taskqueue.h"
#include "globaltask.h"

#include <engines/ep/src/objectregistry.h>
#include <folly/Portability.h>
#include <folly/portability/SysResource.h>
#include <logger/logger.h>
#include <platform/timeutils.h>
#include <chrono>
#include <queue>
#include <sstream>

extern "C" {
static void launch_executor_thread(void* arg) {
    auto* executor = (CB3ExecutorThread*)arg;
    executor->run();
}
}

CB3ExecutorThread::~CB3ExecutorThread() {
    LOG_INFO("Executor killing {}", name);
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
    if (cb_create_named_thread(&thread,
                               launch_executor_thread,
                               this,
                               0,
                               thread_name.c_str()) != 0) {
        std::stringstream ss;
        ss << name.c_str() << ": Initialization error!!!";
        throw std::runtime_error(ss.str().c_str());
    }

    LOG_DEBUG("{}: Started", name);
}

void CB3ExecutorThread::stop(bool wait) {
    if (!wait && (state == EXECUTOR_SHUTDOWN || state == EXECUTOR_DEAD)) {
        return;
    }

    state = EXECUTOR_SHUTDOWN;

    if (!wait) {
        LOG_INFO("{}: Stopping", name);
        return;
    }
    cb_join_thread(thread);
    LOG_INFO("{}: Stopped", name);
}

void CB3ExecutorThread::run() {
    LOG_DEBUG("Thread {} running..", getName());

    // The run loop should not account to any bucket, only once inside a task
    // will accounting be back on. GlobalTask::execute will switch to the bucket
    // and CB3ExecutorPool::cancel will guard the task delete clause with the
    // BucketAllocationGuard to account task deletion to the bucket.
    NonBucketAllocationGuard guard;

    // Set priority based on this thread's task type.
#if defined(__linux__)
    // Only doing this for Linux at present:
    // - On Windows folly's getpriority() compatability function changes the
    //   priority of the entire process.
    // - On macOS setpriority(PRIO_PROCESS) affects the entire process (unlike
    //   Linux where it's only the current thread), hence calling setpriority()
    //   would be pointless.
    if (setpriority(PRIO_PROCESS,
                    /*Current thread*/ 0,
                    ExecutorPool::getThreadPriority(taskType))) {
        LOG_WARNING("Failed to set thread {} to background priority - {}",
                 getName(),
                 strerror(errno));
    } else {
        LOG_INFO("Set thread {} to background priority", getName());
    }
#endif

    priority = getpriority(PRIO_PROCESS, 0);

    for (uint8_t tick = 1;; tick++) {
        if (state != EXECUTOR_RUNNING) {
            break;
        }

        updateCurrentTime();
        if (TaskQueue* q = manager->nextTask(*this, tick)) {
            manager->startWork(taskType);

            if (currentTask->isdead()) {
                manager->doneWork(taskType);
                cancelCurrentTask(*manager);
                continue;
            }

            updateTaskStart();

            const auto curTaskDescr = currentTask->getDescription();
            LOG_TRACE("{}: Run task \"{}\" id {}",
                      getName(),
                      curTaskDescr,
                      currentTask->getId());

            // Now Run the Task ....
            bool again = currentTask->execute(getName());

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

                LOG_TRACE(
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
    std::lock_guard<std::mutex> lh(currentTaskMutex);
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
        std::lock_guard<std::mutex> lh(currentTaskMutex);
        // move currentTask so we 'steal' the pointer and ensure currentTask
        // owns nothing.
        resetThisObject = std::move(currentTask);
    }
    {
        // Freeing of the Task object should be accounted to the engine
        // which owns it.
        BucketAllocationGuard bucketGuard(resetThisObject->getEngine());
        resetThisObject.reset();
    }
}

std::string CB3ExecutorThread::getTaskName() {
    std::lock_guard<std::mutex> lh(currentTaskMutex);
    if (currentTask) {
        return currentTask->getDescription();
    } else {
        return "Not currently running any task";
    }
}

const std::string CB3ExecutorThread::getTaskableName() {
    std::lock_guard<std::mutex> lh(currentTaskMutex);
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
