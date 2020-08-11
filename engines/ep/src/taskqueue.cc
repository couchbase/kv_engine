/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2015 Couchbase, Inc.
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
#include "taskqueue.h"
#include "bucket_logger.h"
#include "cb3_executorpool.h"
#include "cb3_executorthread.h"

#include <cmath>

TaskQueue::TaskQueue(CB3ExecutorPool* m, task_type_t t, const char* nm)
    : name(nm), queueType(t), manager(m), sleepers(0) {
    // EMPTY
}

TaskQueue::~TaskQueue() {
    EP_LOG_DEBUG("Task Queue killing {}", name);
}

const std::string TaskQueue::getName() const {
    return name + to_string(queueType);
}

size_t TaskQueue::getReadyQueueSize() {
    LockHolder lh(mutex);
    return readyQueue.size();
}

size_t TaskQueue::getFutureQueueSize() {
    LockHolder lh(mutex);
    return futureQueue.size();
}

ExTask TaskQueue::_popReadyTask() {
    ExTask t = readyQueue.top();
    readyQueue.pop();
    manager->lessWork(queueType);
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
                mutex.notify_one(); // cond_signal 1
            }
        } else {
            mutex.notify_all(); // cond_broadcast
            numToWake -= sleepers;
        }
    }
}

bool TaskQueue::_doSleep(CB3ExecutorThread& t,
                         std::unique_lock<std::mutex>& lock) {
    t.updateCurrentTime();

    // Determine the time point to wake this thread - either "forever" if the
    // futureQueue is empty, or the earliest wake time in the futureQueue.
    const auto wakeTime = futureQueue.empty()
                                  ? std::chrono::steady_clock::time_point::max()
                                  : futureQueue.top()->getWaketime();

    if (t.getCurTime() < wakeTime && manager->trySleep(queueType)) {
        // Atomically switch from running to sleeping; iff we were previously
        // running.
        executor_state_t expected_state = EXECUTOR_RUNNING;
        if (!t.state.compare_exchange_strong(expected_state,
                                             EXECUTOR_SLEEPING)) {
            return false;
        }
        sleepers++;
        // zzz....
        const auto snooze = wakeTime - t.getCurTime();

        if (snooze > std::chrono::seconds((int)round(MIN_SLEEP_TIME))) {
            mutex.wait_for(lock, MIN_SLEEP_TIME);
        } else {
            mutex.wait_for(lock, snooze);
        }
        // ... woke!
        sleepers--;
        manager->woke();

        // Finished our sleep, atomically switch back to running iff we were
        // previously sleeping.
        expected_state = EXECUTOR_SLEEPING;
        if (!t.state.compare_exchange_strong(expected_state,
                                             EXECUTOR_RUNNING)) {
            return false;
        }
        t.updateCurrentTime();
    }

    return true;
}

bool TaskQueue::_sleepThenFetchNextTask(CB3ExecutorThread& t) {
    std::unique_lock<std::mutex> lh(mutex);
    if (!_doSleep(t, lh)) {
        return false; // shutting down
    }
    return _fetchNextTaskInner(t, lh);
}

bool TaskQueue::_fetchNextTask(CB3ExecutorThread& t) {
    std::unique_lock<std::mutex> lh(mutex);
    return _fetchNextTaskInner(t, lh);
}

bool TaskQueue::_fetchNextTaskInner(CB3ExecutorThread& t,
                                    const std::unique_lock<std::mutex>&) {
    bool ret = false;

    size_t numToWake = _moveReadyTasks(t.getCurTime());

    if (!readyQueue.empty() && readyQueue.top()->isdead()) {
        t.setCurrentTask(_popReadyTask()); // clean out dead tasks first
        ret = true;
    } else if (!readyQueue.empty()) {
        ExTask tid = _popReadyTask(); // and pop out the top task
        t.setCurrentTask(tid);
        ret = true;
    } else {
        numToWake = numToWake ? numToWake - 1 : 0; // 1 fewer task ready
    }

    _doWake_UNLOCKED(numToWake);
    return ret;
}

bool TaskQueue::fetchNextTask(CB3ExecutorThread& thread) {
    NonBucketAllocationGuard guard;
    return _fetchNextTask(thread);
}

bool TaskQueue::sleepThenFetchNextTask(CB3ExecutorThread& thread) {
    NonBucketAllocationGuard guard;
    return _sleepThenFetchNextTask(thread);
}

size_t TaskQueue::_moveReadyTasks(
        const std::chrono::steady_clock::time_point tv) {
    if (!readyQueue.empty()) {
        return 0;
    }

    size_t numReady = 0;
    while (!futureQueue.empty()) {
        ExTask tid = futureQueue.top();
        if (tid->getWaketime() <= tv) {
            futureQueue.pop();
            readyQueue.push(tid);
            numReady++;
        } else {
            break;
        }
    }

    manager->addWork(numReady, queueType);

    // Current thread will pop one task, so wake up one less thread
    return numReady ? numReady - 1 : 0;
}

std::chrono::steady_clock::time_point TaskQueue::_reschedule(ExTask& task) {
    LockHolder lh(mutex);

    futureQueue.push(task);
    return futureQueue.top()->getWaketime();
}

std::chrono::steady_clock::time_point TaskQueue::reschedule(ExTask& task) {
    NonBucketAllocationGuard guard;
    auto rv = _reschedule(task);
    return rv;
}

void TaskQueue::_schedule(ExTask &task) {
    TaskQueue* sleepQ;
    size_t numToWake = 1;

    {
        LockHolder lh(mutex);

        // If we are rescheduling a previously cancelled task, we should reset
        // the task state to the initial value of running.
        task->setState(TASK_RUNNING, TASK_DEAD);

        futureQueue.push(task);

        EP_LOG_TRACE("{}: Schedule a task \"{}\" id {}",
                     name,
                     task->getDescription(),
                     task->getId());

        sleepQ = manager->getSleepQ(queueType);
        _doWake_UNLOCKED(numToWake);
    }
    if (this != sleepQ) {
        sleepQ->doWake(numToWake);
    }
}

void TaskQueue::schedule(ExTask &task) {
    NonBucketAllocationGuard guard;
    _schedule(task);
}

void TaskQueue::_wake(ExTask &task) {
    const std::chrono::steady_clock::time_point now =
            std::chrono::steady_clock::now();
    TaskQueue* sleepQ;
    // One task is being made ready regardless of the queue it's in.
    size_t readyCount = 1;
    {
        LockHolder lh(mutex);
        EP_LOG_DEBUG("{}: Wake a task \"{}\" id {}",
                     name,
                     task->getDescription(),
                     task->getId());

        futureQueue.updateWaketime(task, now);
        task->setState(TASK_RUNNING, TASK_SNOOZED);

        _doWake_UNLOCKED(readyCount);
        sleepQ = manager->getSleepQ(queueType);
    }
    if (this != sleepQ) {
        sleepQ->doWake(readyCount);
    }
}

void TaskQueue::wake(ExTask &task) {
    NonBucketAllocationGuard guard;
    _wake(task);
}
