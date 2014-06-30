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
#include "ep_engine.h"

TaskQueue::TaskQueue(ExecutorPool *m, task_type_t t, const char *nm) :
    name(nm), queueType(t), manager(m)
{
    // EMPTY
}

TaskQueue::~TaskQueue() {
    LOG(EXTENSION_LOG_INFO, "Task Queue killing %s", name.c_str());
}

const std::string TaskQueue::getName() const {
    return (name+taskType2Str(queueType));
}

void TaskQueue::_pushReadyTask(ExTask &tid) {
    readyQueue.push(tid);
    manager->moreWork();
}

ExTask TaskQueue::_popReadyTask(void) {
    ExTask t = readyQueue.top();
    readyQueue.pop();
    manager->lessWork();
    return t;
}

bool TaskQueue::_fetchNextTask(ExTask &task, struct timeval &waketime,
                               task_type_t &taskType, struct timeval now) {
    LockHolder lh(mutex);

    _moveReadyTasks(now);

    if (!futureQueue.empty() &&
        less_tv(futureQueue.top()->waketime, waketime)) {
        waketime = futureQueue.top()->waketime; // record earliest waketime
    }

    if (!readyQueue.empty()) {
        // clean out any dead tasks first
        if (readyQueue.top()->isdead()) {
            task = _popReadyTask();
            return true;
        }
    } else if (pendingQueue.empty()) {
        return false; // return early if tasks are blocked in futureQueue alone
    }

    taskType = manager->tryNewWork(queueType);
    if (taskType != NO_TASK_TYPE) {
        // if this TaskQueue has obtained capacity for the thread, then we must
        // consider any pending tasks too. To ensure prioritized run order,
        // the function below will push any pending task back into
        // the readyQueue (sorted by priority) and pop out top task
        _checkPendingQueue();

        ExTask tid = _popReadyTask();
        if (_checkOutShard(tid)) { // shardLock obtained...
            task = tid; // assign task to thread
            return true;
        } else { // task is now blocked inside shard's pendingQueue (rare)
            manager->doneWork(queueType); // release capacity back to taskQueue
        }
    } else if (!readyQueue.empty()) { // Limit # of threads working on this Q
        ExTask tid = _popReadyTask();
        pendingQueue.push_back(tid);
        // In the future if we wish to limit tasks of other types
        // please remove the assert below
        cb_assert(queueType == AUXIO_TASK_IDX ||
                  queueType == NONIO_TASK_IDX);
    } else { // Let the task continue waiting in pendingQueue
        cb_assert(!pendingQueue.empty());
    }

    return false;
}

bool TaskQueue::fetchNextTask(ExTask &task, struct timeval &waketime,
                              task_type_t &taskType, struct timeval now) {
    EventuallyPersistentEngine *epe = ObjectRegistry::onSwitchThread(NULL, true);
    size_t rv = _fetchNextTask(task, waketime, taskType, now);
    ObjectRegistry::onSwitchThread(epe);
    return rv;
}

void TaskQueue::_moveReadyTasks(struct timeval tv) {
    if (!readyQueue.empty()) {
        return;
    }

    size_t numReady = 0;
    while (!futureQueue.empty()) {
        ExTask tid = futureQueue.top();
        if (less_eq_tv(tid->waketime, tv)) {
            futureQueue.pop();
            readyQueue.push(tid);
            numReady++;
        } else {
            break;
        }
    }

    if (numReady) {
        manager->addWork(numReady);
        // Current thread will pop one task, so wake up one less thread
        manager->wakeSleepers(numReady - 1);
    }
}

void TaskQueue::_checkPendingQueue(void) {
    if (!pendingQueue.empty()) {
        ExTask runnableTask = pendingQueue.front();
        readyQueue.push(runnableTask);
        manager->addWork(1);
        pendingQueue.pop_front();
    }
}

bool TaskQueue::_checkOutShard(ExTask &task) {
    uint16_t shard = task->serialShard;
    if (shard != NO_SHARD_ID) {
        EventuallyPersistentStore *e = task->getEngine()->getEpStore();
        return e->tryLockShard(shard, task);
    }
    return true;
}

void TaskQueue::_doneShard_UNLOCKED(ExTask &task, uint16_t shard,
                                   bool wakeNewWorker) {
    EventuallyPersistentStore *e = task->getEngine()->getEpStore();
    ExTask runnableTask = e->unlockShard(shard);
    if (runnableTask.get() != NULL) {
        readyQueue.push(runnableTask);
        manager->addWork(1);
        if (wakeNewWorker) {
            // dont wake a new thread up if current thread is going to process
            // the same queue when it calls nextTask()
            manager->wakeSleepers(1);
        }
    }
}

void TaskQueue::_doneTask(ExTask &task, task_type_t &curTaskType,
                          bool wakeNewWorker) {
    uint16_t shardSerial = task->serialShard;
    manager->doneWork(curTaskType); // release capacity back to TaskQueue

    if (shardSerial != NO_SHARD_ID) {
        LockHolder lh(mutex);
        _doneShard_UNLOCKED(task, shardSerial, wakeNewWorker);
    }
}

void TaskQueue::doneTask(ExTask &task, task_type_t &curTaskType,
                         bool wakeNewWorker) {
    EventuallyPersistentEngine *epe = ObjectRegistry::onSwitchThread(NULL, true);
    _doneTask(task, curTaskType, wakeNewWorker);
    ObjectRegistry::onSwitchThread(epe);
}

struct timeval TaskQueue::_reschedule(ExTask &task, task_type_t &curTaskType,
                                      bool wakeNewWorker) {
    uint16_t shardSerial = task->serialShard;
    manager->doneWork(curTaskType);

    LockHolder lh(mutex);
    if (shardSerial != NO_SHARD_ID) {
        _doneShard_UNLOCKED(task, shardSerial, wakeNewWorker);
    }

    futureQueue.push(task);
    return futureQueue.top()->waketime;
}

struct timeval TaskQueue::reschedule(ExTask &task, task_type_t &curTaskType,
                                     bool wakeNewWorker) {
    EventuallyPersistentEngine *epe = ObjectRegistry::onSwitchThread(NULL, true);
    struct timeval rv = _reschedule(task, curTaskType, wakeNewWorker);
    ObjectRegistry::onSwitchThread(epe);
    return rv;
}

void TaskQueue::_schedule(ExTask &task) {
    LockHolder lh(mutex);

    futureQueue.push(task);
    // this will wake up conditionally wake up all sleeping threads
    // we need to do this to ensure that a thread does wake up to check this Q
    manager->wakeSleepers(-1);

    LOG(EXTENSION_LOG_DEBUG, "%s: Schedule a task \"%s\" id %d",
            name.c_str(), task->getDescription().c_str(), task->getId());
}

void TaskQueue::schedule(ExTask &task) {
    EventuallyPersistentEngine *epe = ObjectRegistry::onSwitchThread(NULL, true);
    _schedule(task);
    ObjectRegistry::onSwitchThread(epe);
}

void TaskQueue::_wake(ExTask &task) {
    struct  timeval    now;
    gettimeofday(&now, NULL);

    LockHolder lh(mutex);
    LOG(EXTENSION_LOG_DEBUG, "%s: Wake a task \"%s\" id %d", name.c_str(),
            task->getDescription().c_str(), task->getId());

    // MB-9986: Re-sort futureQueue for now. TODO: avoid this O(N) overhead
    std::queue<ExTask> notReady;
    while (!futureQueue.empty()) {
        ExTask tid = futureQueue.top();
        notReady.push(tid);
        futureQueue.pop();
    }

    // Wake thread-count-serialized tasks too
    for (std::list<ExTask>::iterator it = pendingQueue.begin();
         it != pendingQueue.end();) {
        ExTask tid = *it;
        if (tid->getId() == task->getId() || tid->isdead()) {
            notReady.push(tid);
            it = pendingQueue.erase(it);
        } else {
            it++;
        }
    }

    task->waketime = now;

    while (!notReady.empty()) {
        ExTask tid = notReady.front();
        if (less_eq_tv(tid->waketime, now) || tid->isdead()) {
            _pushReadyTask(tid);
        } else {
            futureQueue.push(tid);
        }
        notReady.pop();
    }
}

void TaskQueue::wake(ExTask &task) {
    EventuallyPersistentEngine *epe = ObjectRegistry::onSwitchThread(NULL, true);
    _wake(task);
    ObjectRegistry::onSwitchThread(epe);
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
