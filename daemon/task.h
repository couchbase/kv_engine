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

// Forward decl of the Executor class (used as a friend class)
class Executor;

#include <memcached/types.h>
#include <mutex>
#include <stdexcept>
/**
 * The Task class represents a Task that needs to be performed by the
 * ExecutorPool. See task must be non-blocking and utilize other theads
 * for blocking operations (and notify them with the markRunnable method)
 */
class Task {
public:
    Task() : executor(nullptr) {
        // empty
    }

    Task(const Task&) = delete;

    virtual ~Task() {
        // empty
    }

    /**
     * Execute the task
     *
     * @return true if the task executed completely, or false if the
     *         execution blocked.
     */
    virtual bool execute() = 0;

    /**
     * This method is called by the executor when it is done executing
     * the task (execute returned true, and the executor assumes it
     * is _done_ with the task).
     */
    virtual void notifyExecutionComplete() {
    }

    /**
     * Get the mutex used to protect the task and to ensure that we don't
     * have any race conditions. It should be held when:
     *    * trying to schedule the task
     *    * trying to mark the task runnable
     *
     * The executor will hold the mutex when calling the execute() method
     */
    std::mutex& getMutex() {
        return mutex;
    }

    /**
     * Make the task runnable. this method should _only_ be called
     * from another thread after the task was dispatched during a
     * call to "execute". Note that the mutex must be held while calling
     * this method to avoid race condition.
     */
    void makeRunnable();

private:
    /**
     * The task is pinned to an executor thread while being executed. In
     * order for the executor to pin the task to the thread (and let the
     * task reschedule itself, the executor needs to install a handle
     * to itself). To make sure that no one else use the method we'll
     * make the executor a friend class (if only C++ could let me set
     * which property it should be allowed to touch)
     */
    friend class Executor;

    /**
     * Set the executor that is supposed to handle this task. Once the
     * task is pinned to an executor it cannot be moved.
     *
     * @param executor_ the executor used to run the task
     */
    void setExecutor(Executor *executor_) {
        if (executor != nullptr && executor != executor_) {
            throw std::logic_error("The task can't switch executor");
        }
        executor = executor_;
    }

    /**
     * The executor thread responsible for running this task
     */
    Executor* executor;

    /**
     * The mutex used to ensure that different threads don't race trying
     * to set the tasks internal state
     */
    std::mutex mutex;
};