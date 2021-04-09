/*
 *     Copyright 2013-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#pragma once

#include "globaltask.h"
#include "task_type.h"

#include <platform/platform_thread.h>
#include <atomic>
#include <chrono>
#include <mutex>
#include <utility>

#define TASK_LOG_SIZE 80

#define MIN_SLEEP_TIME 2.0

class CB3ExecutorPool;
class TaskQueue;
class WorkLoadPolicy;

enum executor_state_t {
    EXECUTOR_RUNNING,
    EXECUTOR_WAITING,
    EXECUTOR_SLEEPING,
    EXECUTOR_SHUTDOWN,
    EXECUTOR_DEAD
};

class CB3ExecutorThread {
    friend class CB3ExecutorPool;
    friend class TaskQueue;

public:
    /* The AtomicProcessTime class provides an abstraction for ensuring that
     * changes to a std::chrono::steady_clock::time_point are atomic.  This is
     * achieved by ensuring that all accesses are protected by a mutex.
     */
    class AtomicProcessTime {
    public:
        AtomicProcessTime() = default;
        explicit AtomicProcessTime(
                const std::chrono::steady_clock::time_point& tp)
            : timepoint(tp) {
        }

        void setTimePoint(const std::chrono::steady_clock::time_point& tp) {
            std::lock_guard<std::mutex> lock(mutex);
            timepoint = tp;
        }

        std::chrono::steady_clock::time_point getTimePoint() const {
            std::lock_guard<std::mutex> lock(mutex);
            return timepoint;
        }

    private:
        mutable std::mutex mutex;
        std::chrono::steady_clock::time_point timepoint;
    };

    CB3ExecutorThread(CB3ExecutorPool* m,
                      task_type_t type,
                      const std::string nm)
        : manager(m),
          taskType(type),
          name(nm),
          state(EXECUTOR_RUNNING),
          now(std::chrono::steady_clock::now()),
          taskStart(),
          currentTask(nullptr) {
    }

    ~CB3ExecutorThread();

    void start();

    void run();

    void stop(bool wait = true);

    void schedule(ExTask& task);

    void wake(ExTask& task);

    // Changes this threads' current task to the specified task
    void setCurrentTask(ExTask newTask);

    /**
     * Reset the currentTask shared_ptr so it 'owns' nothing
     */
    void resetCurrentTask();

    const std::string& getName() const {
        return name;
    }

    std::string getTaskName();

    const std::string getTaskableName();

    std::chrono::steady_clock::time_point getTaskStart() const {
        return taskStart.getTimePoint();
    }

    void updateTaskStart() {
        const std::chrono::steady_clock::time_point& now =
                std::chrono::steady_clock::now();
        taskStart.setTimePoint(now);
        currentTask->updateLastStartTime(now);
    }

    const std::string getStateName();

    std::chrono::steady_clock::time_point getCurTime() const {
        return now.getTimePoint();
    }

    void updateCurrentTime() {
        now.setTimePoint(std::chrono::steady_clock::now());
    }

    /// @return the threads' type.
    task_type_t getTaskType() const;

    /// Return the threads' OS priority.
    int getPriority() const;

protected:
    void cancelCurrentTask(CB3ExecutorPool& manager);

    cb_thread_t thread;
    CB3ExecutorPool* manager;
    task_type_t taskType;
    const std::string name;
    std::atomic<executor_state_t> state;

    // record of current time
    AtomicProcessTime now;
    AtomicProcessTime taskStart;

    std::mutex currentTaskMutex; // Protects currentTask
    ExTask currentTask;

    // OS priority of the thread. Only available once the thread
    // has been started.
    int priority = 0;
};
