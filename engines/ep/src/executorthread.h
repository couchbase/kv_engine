/*
 *     Copyright 2013 Couchbase, Inc.
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

#include "config.h"

#include "globaltask.h"
#include "objectregistry.h"
#include "task_type.h"

#include <platform/ring_buffer.h>
#include <platform/processclock.h>
#include <relaxed_atomic.h>

#include <atomic>
#include <chrono>
#include <deque>
#include <list>
#include <map>
#include <mutex>
#include <queue>
#include <string>
#include <utility>
#include <vector>

#define TASK_LOG_SIZE 80

#define MIN_SLEEP_TIME 2.0

class ExecutorPool;
class ExecutorThread;
class TaskQueue;
class WorkLoadPolicy;

enum executor_state_t {
    EXECUTOR_RUNNING,
    EXECUTOR_WAITING,
    EXECUTOR_SLEEPING,
    EXECUTOR_SHUTDOWN,
    EXECUTOR_DEAD
};


class ExecutorThread {
    friend class ExecutorPool;
    friend class TaskQueue;
public:

    /* The AtomicProcessTime class provides an abstraction for ensuring that
     * changes to a ProcessClock::time_point are atomic.  This is achieved by
     * ensuring that all accesses are protected by a mutex.
     */
    class AtomicProcessTime {
    public:
        AtomicProcessTime() {}
        AtomicProcessTime(const ProcessClock::time_point& tp) : timepoint(tp) {}

        void setTimePoint(const ProcessClock::time_point& tp) {
            std::lock_guard<std::mutex> lock(mutex);
            timepoint = tp;
        }

        ProcessClock::time_point getTimePoint() const {
            std::lock_guard<std::mutex> lock(mutex);
            return timepoint;
        }

    private:
        mutable std::mutex mutex;
        ProcessClock::time_point timepoint;
    };

    ExecutorThread(ExecutorPool* m, task_type_t type, const std::string nm)
        : manager(m),
          taskType(type),
          name(nm),
          state(EXECUTOR_RUNNING),
          now(ProcessClock::now()),
          taskStart(),
          currentTask(NULL) {
    }

    ~ExecutorThread() {
        LOG(EXTENSION_LOG_INFO, "Executor killing %s", name.c_str());
    }

    void start(void);

    void run(void);

    void stop(bool wait=true);

    void schedule(ExTask &task);

    void reschedule(ExTask &task);

    void wake(ExTask &task);

    // Changes this threads' current task to the specified task
    void setCurrentTask(ExTask newTask);

    /**
     * Reset the currentTask shared_ptr so it 'owns' nothing
     */
    void resetCurrentTask();

    const std::string& getName() const { return name; }

    std::string getTaskName();

    const std::string getTaskableName();

    ProcessClock::time_point getTaskStart() const {
        return taskStart.getTimePoint();
    }

    void updateTaskStart(void) {
        const ProcessClock::time_point& now = ProcessClock::now();
        taskStart.setTimePoint(now);
        currentTask->updateLastStartTime(now);
    }

    const std::string getStateName();

    ProcessClock::time_point getCurTime() const {
        return now.getTimePoint();
    }

    void updateCurrentTime(void) {
        now.setTimePoint(ProcessClock::now());
    }

protected:

    cb_thread_t thread;
    ExecutorPool *manager;
    task_type_t taskType;
    const std::string name;
    std::atomic<executor_state_t> state;

    // record of current time
    AtomicProcessTime now;
    AtomicProcessTime taskStart;

    std::mutex currentTaskMutex; // Protects currentTask
    ExTask currentTask;
};
