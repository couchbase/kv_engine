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
#include "executorthread.h"

TaskQueue::TaskQueue(ExecutorPool *m, task_type_t t, const char *nm) :
    name(nm), queueType(t), manager(m), sleepers(0)
{
    // EMPTY
}

TaskQueue::~TaskQueue() {
    LOG(EXTENSION_LOG_INFO, "Task Queue killing %s", name.c_str());
}

const std::string TaskQueue::getName() const {
    return (name+taskType2Str(queueType));
}

ExTask TaskQueue::_popReadyTask(void) {
    ExTask t = readyQueue.top();
    readyQueue.pop();
    manager->lessWork();
    return t;
}

void TaskQueue::doWake(size_t &numToWake) {
    LockHolder lh(mutex);
    _doWake_UNLOCKED(numToWake);
}

void TaskQueue::_doWake_UNLOCKED(size_t &numToWake) {
    if (sleepers && numToWake)  {
        if (numToWake < sleepers) {
            for (; numToWake; --numToWake) {
                mutex.notifyOne(); // cond_signal 1
            }
        } else {
            mutex.notify(); // cond_broadcast
            numToWake -= sleepers;
        }
    }
}

bool TaskQueue::_doSleep(ExecutorThread &t) {
    gettimeofday(&t.now, NULL);
    if (less_tv(t.now, t.waketime) && manager->trySleep()) {
        if (t.state == EXECUTOR_RUNNING) {
            t.state = EXECUTOR_SLEEPING;
        } else {
            return false;
        }
        sleepers++;
        // zzz....
        if (is_max_tv(t.waketime)) {
            advance_tv(t.now, MIN_SLEEP_TIME); // timed sleeps to void missing
            mutex.wait(t.now);                 // posts
        } else {
            mutex.wait(t.waketime);
        }
        // ... woke!
        sleepers--;
        manager->woke();

        if (t.state == EXECUTOR_SLEEPING) {
            t.state = EXECUTOR_RUNNING;
        } else {
            return false;
        }
        gettimeofday(&t.now, NULL);
    }
    set_max_tv(t.waketime);
    return true;
}

bool TaskQueue::_fetchNextTask(ExecutorThread &t, bool toSleep) {
    bool ret = false;
    LockHolder lh(mutex);

    if (toSleep && !_doSleep(t)) {
        return ret; // shutting down
    }

    size_t numToWake = _moveReadyTasks(t.now);

    if (!futureQueue.empty() && t.startIndex == queueType &&
        less_tv(futureQueue.top()->waketime, t.waketime)) {
        t.waketime = futureQueue.top()->waketime; // record earliest waketime
    }

    if (!readyQueue.empty() && readyQueue.top()->isdead()) {
        t.currentTask = _popReadyTask(); // clean out dead tasks first
        ret = true;
    } else if (!readyQueue.empty() || !pendingQueue.empty()) {
        t.curTaskType = manager->tryNewWork(queueType);
        if (t.curTaskType != NO_TASK_TYPE) {
            // if this TaskQueue has obtained capacity for the thread, then we must
            // consider any pending tasks too. To ensure prioritized run order,
            // the function below will push any pending task back into
            // the readyQueue (sorted by priority)
            _checkPendingQueue();

            ExTask tid = _popReadyTask(); // and pop out the top task
            t.currentTask = tid; // assign task to thread
            ret = true;
        } else if (!readyQueue.empty()) { // We hit limit on max # workers
            ExTask tid = _popReadyTask(); // that can work on current Q type!
            pendingQueue.push_back(tid);
            numToWake = numToWake ? numToWake-- : 0; // 1 fewer task ready
        } else { // Let the task continue waiting in pendingQueue
            cb_assert(!pendingQueue.empty());
            numToWake = numToWake ? numToWake-- : 0; // 1 fewer task ready
        }
    }

    _doWake_UNLOCKED(numToWake);
    lh.unlock();

    if (numToWake) {
        manager->wakeMore(numToWake, this);
    }

    return ret;
}

bool TaskQueue::fetchNextTask(ExecutorThread &thread, bool toSleep) {
    EventuallyPersistentEngine *epe = ObjectRegistry::onSwitchThread(NULL, true);
    size_t rv = _fetchNextTask(thread, toSleep);
    ObjectRegistry::onSwitchThread(epe);
    return rv;
}

size_t TaskQueue::_moveReadyTasks(struct timeval tv) {
    if (!readyQueue.empty()) {
        return 0;
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

    manager->addWork(numReady);

    // Current thread will pop one task, so wake up one less thread
    return numReady ? numReady - 1 : 0;
}

void TaskQueue::_checkPendingQueue(void) {
    if (!pendingQueue.empty()) {
        ExTask runnableTask = pendingQueue.front();
        readyQueue.push(runnableTask);
        manager->addWork(1);
        pendingQueue.pop_front();
    }
}

struct timeval TaskQueue::_reschedule(ExTask &task, task_type_t &curTaskType) {
    struct timeval waktime;
    manager->doneWork(curTaskType);

    LockHolder lh(mutex);

    futureQueue.push(task);
    if (curTaskType == queueType) {
        waktime = futureQueue.top()->waketime;
    } else {
        set_max_tv(waktime);
    }

    return waktime;
}

struct timeval TaskQueue::reschedule(ExTask &task, task_type_t &curTaskType) {
    EventuallyPersistentEngine *epe = ObjectRegistry::onSwitchThread(NULL, true);
    struct timeval rv = _reschedule(task, curTaskType);
    ObjectRegistry::onSwitchThread(epe);
    return rv;
}

void TaskQueue::_schedule(ExTask &task) {
    LockHolder lh(mutex);

    futureQueue.push(task);

    LOG(EXTENSION_LOG_DEBUG, "%s: Schedule a task \"%s\" id %d",
            name.c_str(), task->getDescription().c_str(), task->getId());

    size_t numToWake = 1;
    _doWake_UNLOCKED(numToWake);
    if (numToWake) {
        lh.unlock();
        manager->wakeMore(numToWake, this); // look for sleepers in other Qs
    }
}

void TaskQueue::schedule(ExTask &task) {
    EventuallyPersistentEngine *epe = ObjectRegistry::onSwitchThread(NULL, true);
    _schedule(task);
    ObjectRegistry::onSwitchThread(epe);
}

void TaskQueue::_wake(ExTask &task) {
    struct  timeval    now;
    size_t numReady = 0;
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

    // Note that this task that we are waking may nor may not be blocked in Q
    task->waketime = now;
    task->setState(TASK_RUNNING, TASK_SNOOZED);

    while (!notReady.empty()) {
        ExTask tid = notReady.front();
        if (less_eq_tv(tid->waketime, now) || tid->isdead()) {
            readyQueue.push(tid);
            numReady++;
        } else {
            futureQueue.push(tid);
        }
        notReady.pop();
    }

    if (numReady) {
        manager->addWork(numReady);
        _doWake_UNLOCKED(numReady);
        if (numReady) {
            lh.unlock();
            manager->wakeMore(numReady, this);
        }
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
