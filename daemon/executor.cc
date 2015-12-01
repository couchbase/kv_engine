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

#include "executor.h"
#include "task.h"

#include <iostream>

Executor::~Executor() {
    shutdown = true;
    std::unique_lock<std::mutex> lock(mutex);
    idlecond.notify_all();
    // Wait until the thread stops
    while (running) {
        shutdowncond.wait(lock);
    }
}

void Executor::run() {
    running = true;

    {
        // make sure that the creator is waiting for us
        std::lock_guard<std::mutex> lock(mutex);
    }
    // notify the dude that we've actually started!!!
    idlecond.notify_all();

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

        {
            // Lock the task so no one else can touch it and we won't
            // have any races..
            std::lock_guard<std::mutex> guard(task->getMutex());
            if (task->execute()) {
                task->notifyExecutionComplete();
            } else {
                // put it in the wait-queue.. We need the lock for the waitq
                lock.lock();
                waitq[task.get()] = task;
                lock.unlock();
            }
        }
    }

    // wait for the wait-queue to drain...
    std::unique_lock<std::mutex> lock(mutex);
    while (!waitq.empty()) {
        idlecond.wait(lock);
    }

    running = false;
    shutdowncond.notify_one();
}

void Executor::schedule(const std::shared_ptr<Task>& task, bool runnable) {
    std::lock_guard<std::mutex> guard(mutex);
    task->setExecutor(this);

    if (runnable) {
        runq.push(task);
        idlecond.notify_one();
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
    idlecond.notify_one();
}

std::unique_ptr<Executor> createWorker() {
    auto *executor = new Executor;
    executor->start();
    return std::unique_ptr<Executor>(executor);
}
