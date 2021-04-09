/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2015-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "executor.h"
#include "task.h"

#include <algorithm>
#include <iostream>

Executor::~Executor() {
    std::unique_lock<std::mutex> lock(mutex);
    shutdown = true;
    idlecond.notify_all();
    // Wait until the thread stops
    while (running) {
        shutdowncond.wait(lock);
    }
    waitForState(Couchbase::ThreadState::Zombie);
}

void Executor::run() {
    running = true;
    // According to the spec we have to call setRunning (that'll notify the
    // the thread calling start())
    setRunning();

    while (true) {
        std::unique_lock<std::mutex> lock(mutex);
        while (!shutdown && runq.empty()) {
            idlecond.wait(lock);
        }

        if (shutdown) {
            break;
        }

        auto task = runq.front();
        runq.pop();

        // Release the lock so that others may schedule new events
        lock.unlock();

        // Lock the task so no one else can touch it and we won't
        // have any races..
        task->getMutex().lock();
        if (task->execute() == Task::Status::Finished) {
            // Unlock the mutex, we're not going to use this anymore
            // By not holding the mutex in notifyExecutionComplete
            // we won't get any warnings from ThreadSanitizer by
            // locking in oposite order (typically you hold the thread
            // mutex when you create a task, and then aqcuire the task
            // lock. This time we aqcuired the task mutex first and the
            // notification method will try to grab the thread lock later
            // on..
            task->getMutex().unlock();

            // tell the task that the executor consider it done with the
            // task and will no longer operate on it.
            task->notifyExecutionComplete();
        } else {
            // put it in the wait-queue.. We need the lock for the waitq
            lock.lock();
            waitq[task.get()] = task;
            lock.unlock();
            // Release the task lock so that the backend thread may start
            // using it
            task->getMutex().unlock();
        }
    }

    // move all items in future-queue out of the wait-queue
    for (const auto& ftask : futureq) {
        std::lock_guard<std::mutex> guard(ftask.first->getMutex());
        makeRunnable(ftask.first);
    }

    // wait for the wait-queue to drain...
    std::unique_lock<std::mutex> lock(mutex);
    while (!waitq.empty()) {
        idlecond.wait(lock);
    }

    running = false;
    shutdowncond.notify_all();
}

void Executor::schedule(const std::shared_ptr<Task>& task, bool runnable) {
    std::lock_guard<std::mutex> guard(mutex);
    task->setExecutor(this);

    if (runnable) {
        runq.push(task);
        idlecond.notify_all();
    } else {
        waitq[task.get()] = task;
    }
}

void Executor::makeRunnable(Task* task) {
    if (task->getMutex().try_lock()) {
        task->getMutex().unlock();
        throw std::logic_error(
            "The mutex should be held when trying to reschedule a event");
    }

    std::lock_guard<std::mutex> guard(mutex);
    auto iter = waitq.find(task);
    if (iter == waitq.end()) {
        throw std::runtime_error("Internal error object is not in the waitq");
    }
    runq.push(iter->second);
    waitq.erase(iter);
    idlecond.notify_all();
}

void Executor::makeRunnable(Task& task,
                            std::chrono::steady_clock::time_point time) {
    if (task.getMutex().try_lock()) {
        task.getMutex().unlock();
        throw std::logic_error(
                "The mutex should be held when trying to reschedule a event");
    }

    std::lock_guard<std::mutex> guard(mutex);
    futureq.emplace_back(&task, time);
}

void Executor::clockTick() {
    std::vector<Task*> wakeableTasks;

    {
        std::lock_guard<std::mutex> guard(mutex);
        auto now = clock.now();

        futureq.erase(
                std::remove_if(futureq.begin(),
                               futureq.end(),
                               [&now, &wakeableTasks](FutureTask& ftask) {
                                   if (ftask.second <= now) {
                                       wakeableTasks.push_back(ftask.first);
                                       return true;
                                   }
                                   return false;
                               }),
                futureq.end());
    }

    // Need to do this without holding the executor lock to avoid lock inversion
    for (auto* task : wakeableTasks) {
        std::lock_guard<std::mutex> guard(task->getMutex());
        makeRunnable(task);
    }
}

size_t Executor::waitqSize() const {
    std::lock_guard<std::mutex> guard(mutex);
    return waitq.size();
}

size_t Executor::runqSize() const {
    std::lock_guard<std::mutex> guard(mutex);
    return runq.size();
}

size_t Executor::futureqSize() const {
    std::lock_guard<std::mutex> guard(mutex);
    return futureq.size();
}

std::unique_ptr<Executor> createWorker(cb::ProcessClockSource& clock) {
    auto* executor = new Executor(clock);
    executor->start();
    return std::unique_ptr<Executor>(executor);
}
