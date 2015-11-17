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

#include <atomic>
#include <condition_variable>
#include <memory>
#include <platform/platform.h>
#include <queue>
#include <unordered_map>

/**
 * Forward decl of the Task to avoid circular dependencies
 */
class Task;

class Executor;

/**
 * The Executor class represents a single executor thread. It keeps
 * working on items in the runq and whenever a request can't be completed
 * it is put on the waitq until someone makes the object runnable by calling
 * makeRunnable (NOTE: you should hold the command's lock when calling that),
 * and it is NOT allowed to call it from any of the executors threads (that
 * may create deadlock)
 */
class Executor {
public:
    /**
     * Initialize the Executor object
     */
    Executor() :
          tid(cb_thread_t(0)) {
        shutdown.store(false);
        running.store(false);
    }

    Executor(const Executor&) = delete;

    /**
     * Terminate the executor thread
     */
    virtual ~Executor();

    /**
     * Schedule a task for execution at some time
     */
    void schedule(const std::shared_ptr<Task>& command, bool runnable);

    /**
     * Make the task runnable
     */
    void makeRunnable(Task* task);

    /**
     * Start the executor thread. This should not be called by anyone, its
     * just the annoying fact that cb_create_thread needs a c function, and
     * I need a way to get into this object (I could of course subclass this
     * with a private class in executors.cc and have this method protected,
     * but thats a bit overkill.. you've read the comment here, don't use it)
     */
    void run();

protected:
    // The createWorker needs to wait for the executor to properly start
    friend std::unique_ptr<Executor> createWorker();

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
    std::mutex mutex;

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
     * The thread identifier
     */
    cb_thread_t tid;
};

std::unique_ptr<Executor> createWorker();
