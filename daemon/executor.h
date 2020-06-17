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
#pragma once

#include <platform/processclock.h>
#include <platform/thread.h>

#include <atomic>
#include <condition_variable>
#include <memory>
#include <queue>
#include <unordered_map>

/**
 * Forward decl of the Task to avoid circular dependencies
 */
class Task;

/**
 * The Executor class represents a single executor thread. It keeps
 * working on items in the runq and whenever a request can't be completed
 * it is put on the waitq until someone makes the object runnable by calling
 * makeRunnable (NOTE: you should hold the command's lock when calling that),
 * and it is NOT allowed to call it from any of the executors threads (that
 * may create deadlock)
 */
class Executor : public Couchbase::Thread {
public:
    /**
     * Initialize the Executor object
     */
    explicit Executor(cb::ProcessClockSource& clock)
        : Couchbase::Thread("mc:executor"), clock(clock) {
        shutdown.store(false);
        running.store(false);
    }

    Executor(const Executor&) = delete;

    /**
     * Terminate the executor thread
     */
    ~Executor() override;

    /**
     * Schedule a task for execution at some time
     */
    void schedule(const std::shared_ptr<Task>& command, bool runnable);

    /**
     * Make the task runnable
     */
    void makeRunnable(Task* task);

    /**
     * Schedule the task to be made runnable in the future
     */
    void makeRunnable(Task& task, std::chrono::steady_clock::time_point time);

    /**
     * This method should be called periodically (usually by the ExecutorPool)
     * to move tasks in the futureq that are ready into the runq.
     */
    void clockTick();

    size_t waitqSize() const;

    size_t runqSize() const;

    size_t futureqSize() const;

protected:
    void run() override;

    /**
     * Is shutdown requested?
     */
    std::atomic_bool shutdown;
    /**
     * Is the threads running?
     */
    std::atomic_bool running;

    /**
     * All the data structures (and condition variables) is using this
     * single lock...
     *
     * @todo measure and refactor if needed
     */
    mutable std::mutex mutex;

    /**
     * The FIFO queue of commands ready to run
     */
    std::queue<std::shared_ptr<Task> > runq;

    /**
     * When a task is being served by a backend thread it is put in
     * the "wait queue". These commands may be runnable in any given order
     * so we need an easy way to locate them. I figured using an
     * unordered_map would give us easy search access ;)
     *
     * @todo measure and refactor if needed
     */
    std::unordered_map<Task*, std::shared_ptr<Task> > waitq;

    using FutureTask = std::pair<Task*, std::chrono::steady_clock::time_point>;

    /**
     * If a task needs to be be runnable at a specific time in the future
     * then it is placed in the "future queue" with its time (at the same
     * time as being in the wait queue).
     *
     * This vector *could* justifiably be sorted by time to reduce how
     * much of it needs processing each time. But vectors are fast so it
     * shouldn't be noticeable for the small size this should be.
     */
    std::vector<FutureTask> futureq;

    /**
     * When the runqueue is empty the executor thread blocks on this condition
     * variable to avoid consuming any CPU
     */
    std::condition_variable idlecond;

    /**
     * The destructor blocks on this condition variable while waiting for the
     * thread to shut down.
     */
    std::condition_variable shutdowncond;

    /**
     * The source to use for getting 'now' for the std::chrono::steady_clock.
     * Mostly intended for allowing mocking of the time source in testing.
     */
    cb::ProcessClockSource& clock;
};

std::unique_ptr<Executor> createWorker(cb::ProcessClockSource& clock);
