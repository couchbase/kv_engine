/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2016 Couchbase, Inc.
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

#include <array>
#include <platform/processclock.h>
#include <platform/sized_buffer.h>

#include "atomic.h"
#include "config.h"
#include "task_type.h"

enum task_state_t {
    TASK_RUNNING,
    TASK_SNOOZED,
    TASK_DEAD
};

std::string to_string(task_state_t state);

enum class TaskId : int {
#define TASK(name, type, prio) name,
#include "tasks.def.h"
#undef TASK
    TASK_COUNT
};

typedef int queue_priority_t;

enum class TaskPriority : int {
#define TASK(name, type, prio) name = prio,
#include "tasks.def.h"
#undef TASK
    PRIORITY_COUNT
};

class Taskable;
class EventuallyPersistentEngine;

class GlobalTask : public RCValue {
friend class CompareByDueDate;
friend class CompareByPriority;
friend class ExecutorPool;
friend class ExecutorThread;
public:

    GlobalTask(Taskable& t,
               TaskId taskId,
               double sleeptime = 0,
               bool completeBeforeShutdown = true);

    GlobalTask(EventuallyPersistentEngine *e,
               TaskId taskId,
               double sleeptime = 0,
               bool completeBeforeShutdown = true);

    /* destructor */
    virtual ~GlobalTask(void) {
    }

    /**
     * The invoked function when the task is executed.
     *
     * @return Whether or not this task should be rescheduled
     */
    virtual bool run(void) = 0;

    /**
     * Gives a description of this task.
     *
     * @return A description of this task
     */
    virtual cb::const_char_buffer getDescription() = 0;

    virtual int maxExpectedDuration(void) {
        return 3600;
    }

    /**
     * test if a task is dead
     */
     bool isdead(void) {
        return (state == TASK_DEAD);
     }


    /**
     * Cancels this task by marking it dead.
     */
    void cancel(void) {
        state = TASK_DEAD;
    }

    /**
     * Puts the task to sleep for a given duration.
     */
    virtual void snooze(const double secs);

    /**
     * Returns the id of this task.
     *
     * @return A unique task id number.
     */
    size_t getId() const { return uid; }

    /**
     * Returns the type id of this task.
     *
     * @return A type id of the task.
     */
    TaskId getTypeId() { return typeId; }

    /**
     * Gets the engine that this task was scheduled from
     *
     * @returns A handle to the engine
     */
    EventuallyPersistentEngine* getEngine() { return engine; }

    task_state_t getState(void) {
        return state.load();
    }

    void setState(task_state_t tstate, task_state_t expected) {
        state.compare_exchange_strong(expected, tstate);
    }

    Taskable& getTaskable() const {
        return taskable;
    }

    ProcessClock::time_point getWaketime() const {
        const auto waketime_chrono = std::chrono::nanoseconds(waketime);
        return ProcessClock::time_point(waketime_chrono);
    }

    void updateWaketime(ProcessClock::time_point tp) {
        waketime = to_ns_since_epoch(tp).count();
    }

    void updateWaketimeIfLessThan(ProcessClock::time_point tp) {
        const int64_t tp_ns = to_ns_since_epoch(tp).count();
        atomic_setIfBigger(waketime, tp_ns);
    }

    ProcessClock::time_point getLastStartTime() const {
        const auto waketime_chrono = std::chrono::nanoseconds(lastStartTime);
        return ProcessClock::time_point(waketime_chrono);
    }

    void updateLastStartTime(ProcessClock::time_point tp) {
        lastStartTime = to_ns_since_epoch(tp).count();
    }

    ProcessClock::duration getTotalRuntime() const {
        return std::chrono::nanoseconds(totalRuntime);
    }

    void updateRuntime(ProcessClock::duration tp) {
        totalRuntime += std::chrono::duration_cast<std::chrono::nanoseconds>(tp)
                                .count();
    }

    queue_priority_t getQueuePriority() const {
        return static_cast<queue_priority_t>(priority);
    }

    /*
     * Lookup the task name for TaskId id.
     * The data used is generated from tasks.def.h
     */
    static const char* getTaskName(TaskId id);

    /*
     * Lookup the task priority for TaskId id.
     * The data used is generated from tasks.def.h
     */
    static TaskPriority getTaskPriority(TaskId id);

    /*
     * Lookup the task type for TaskId id.
     * The data used is generated from tasks.def.h
     */
    static task_type_t getTaskType(TaskId id);

    /*
     * A vector of all TaskId generated from tasks.def.h
     */
    static std::array<TaskId, static_cast<int>(TaskId::TASK_COUNT)> allTaskIds;

protected:
    /**
     * We are using a int64_t as opposed to ProcessTime::time_point because we
     * want the access to be atomic without the use of a mutex. The reason for
     * this is that we update these timepoints in locations which have been
     * shown to be pretty hot (e.g. CompareByDueDate) and we want to avoid
     * the overhead of acquiring mutexes.
     */
    using atomic_time_point = std::atomic<int64_t>;
    using atomic_duration = std::atomic<int64_t>;
    bool blockShutdown;
    std::atomic<task_state_t> state;
    const size_t uid;
    const TaskId typeId;
    TaskPriority priority;
    EventuallyPersistentEngine *engine;
    Taskable& taskable;

    static std::atomic<size_t> task_id_counter;
    static size_t nextTaskId() { return task_id_counter.fetch_add(1); }

    atomic_duration totalRuntime;
    atomic_time_point lastStartTime;

private:
    atomic_time_point waketime; // used for priority_queue
};

typedef SingleThreadedRCPtr<GlobalTask> ExTask;

/**
 * Order tasks by their priority and taskId (try to ensure FIFO)
 * @return true if t2 should have priority over t1
 */
class CompareByPriority {
public:
    bool operator()(ExTask &t1, ExTask &t2) {
        return (t1->getQueuePriority() == t2->getQueuePriority()) ?
               (t1->uid > t2->uid) :
               (t1->getQueuePriority() > t2->getQueuePriority());
    }
};

/**
 * Order tasks by their ready date.
 * @return true if t2 should have priority over t1
 */
class CompareByDueDate {
public:
    bool operator()(ExTask &t1, ExTask &t2) {
        return t2->waketime < t1->waketime;
    }
};
