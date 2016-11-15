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

#ifndef SRC_SCHEDULER_H_
#define SRC_SCHEDULER_H_ 1

#include "config.h"

#include <chrono>
#include <deque>
#include <list>
#include <map>
#include <mutex>
#include <queue>
#include <string>
#include <utility>
#include <vector>

#include <platform/processclock.h>
#include <relaxed_atomic.h>

#include "atomic.h"
#include "mutex.h"
#include "objectregistry.h"
#include "tasks.h"
#include "task_type.h"
#include "tasklogentry.h"
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

    ExecutorThread(ExecutorPool *m, int startingQueue,
                   const std::string nm) : manager(m),
          startIndex(startingQueue), name(nm),
          state(EXECUTOR_RUNNING),
          now(ProcessClock::now()),
          waketime(ProcessClock::time_point::max()),
          taskStart(),
          currentTask(NULL), curTaskType(NO_TASK_TYPE),
          tasklog(TASK_LOG_SIZE), slowjobs(TASK_LOG_SIZE) {}

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

    const std::string& getName() const { return name; }

    const std::string getTaskName() {
        LockHolder lh(currentTaskMutex);
        if (currentTask) {
            return currentTask->getDescription();
        } else {
            return std::string("Not currently running any task");
        }
    }

    const std::string getTaskableName() {
        LockHolder lh(currentTaskMutex);
        if (currentTask) {
            return currentTask->getTaskable().getName();
        } else {
            return std::string();
        }
    }

    ProcessClock::time_point getTaskStart() const {
        return taskStart.getTimePoint();
    }

    void updateTaskStart(void) {
        taskStart.setTimePoint(ProcessClock::now());
    }

    const std::string getStateName();

    void addLogEntry(const std::string &desc, const task_type_t taskType,
                     const ProcessClock::duration runtime,
                     rel_time_t startRelTime, bool isSlowJob);

    const std::vector<TaskLogEntry> getLog() {
        LockHolder lh(logMutex);
        return tasklog.contents();
    }

    const std::vector<TaskLogEntry> getSlowLog() {
        LockHolder lh(logMutex);
        return slowjobs.contents();
    }

    ProcessClock::time_point getWaketime() const {
        return waketime.getTimePoint();
    }

    void setWaketime(const ProcessClock::time_point tp) {
        waketime.setTimePoint(tp);
    }

    ProcessClock::time_point getCurTime() const {
        return now.getTimePoint();
    }

    void updateCurrentTime(void) {
        now.setTimePoint(ProcessClock::now());
    }

protected:

    cb_thread_t thread;
    ExecutorPool *manager;
    int startIndex;
    const std::string name;
    AtomicValue<executor_state_t> state;

    // record of current time
    AtomicProcessTime now;
    // record of the earliest time the task can be woken-up
    AtomicProcessTime waketime;
    AtomicProcessTime taskStart;

    Mutex currentTaskMutex; // Protects currentTask
    ExTask currentTask;

    task_type_t curTaskType;

    Mutex logMutex;
    RingBuffer<TaskLogEntry> tasklog;
    RingBuffer<TaskLogEntry> slowjobs;
};

#endif  // SRC_SCHEDULER_H_
