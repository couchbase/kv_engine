/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2014 Couchbase, Inc.
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

#include "taskqueue.h"
#include "executorpool.h"

TaskQueue::TaskQueue(ExecutorPool *m, task_type_t t, const char *nm) :
    isLock(false), name(nm), hasWokenTask(false), queueType(t), manager(m),
    tasklog(TASK_LOG_SIZE), slowjobs(TASK_LOG_SIZE)
{
    // EMPTY
}

TaskQueue::~TaskQueue() {
    LOG(EXTENSION_LOG_INFO, "Task Queue killing %s", name.c_str());
}

const std::string TaskQueue::getName() const {
    return (name+taskType2Str(queueType));
}

bool TaskQueue::empty(void) {
    return readyQueue.empty() && futureQueue.empty();
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
                              int &taskType, struct timeval now) {
    bool inverse = false;
    if (!isLock.compare_exchange_strong(inverse, true)) {
        return false;
    }

    inverse = true;
    LockHolder lh(mutex);

    if (empty()) {
        isLock.compare_exchange_strong(inverse, false);
        return false;
    }

    moveReadyTasks(now);

    if (!futureQueue.empty() &&
        less_tv(futureQueue.top()->waketime, waketime)) {
        waketime = futureQueue.top()->waketime; // record earliest waketime
    }

    manager->doneWork(taskType);

    if (!readyQueue.empty()) {
        if (readyQueue.top()->isdead()) {
            task = popReadyTask();
            isLock.compare_exchange_strong(inverse, false);
            return true;
        }
        taskType = manager->tryNewWork(queueType);
        if (taskType != NO_TASK_TYPE) {
            task = popReadyTask();
            isLock.compare_exchange_strong(inverse, false);
            return true;
        }
    }

    isLock.compare_exchange_strong(inverse, false);
    return false;
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
    manager->notifyAll();

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
    manager->notifyAll();
}

void TaskQueue::addLogEntry(const std::string &desc, const hrtime_t runtime,
                            rel_time_t t, bool isSlowJob) {
    TaskLogEntry tle(desc, runtime, t);
    LockHolder lh(mutex);
    tasklog.add(tle);
    if (isSlowJob) {
        slowjobs.add(tle);
    }
}

const std::string TaskQueue::taskType2Str(task_type_t type) {
    switch (type) {
    case WRITER_TASK_IDX:
        return std::string("Writer");
    case READER_TASK_IDX:
        return std::string("Reader");
    case AUXIO_TASK_IDX:
        return std::string("AuxIO");
    case NONIO_TASK_IDX:
        return std::string("NonIO");
    default:
        return std::string("None");
    }
}
