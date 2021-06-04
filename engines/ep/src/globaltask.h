/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2016-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#pragma once

#include "atomic.h"
#include "task_type.h"

#include <platform/processclock.h>
#include <array>

enum task_state_t {
    /// Runnable/Running: Task is ready to run (but awaiting a thread to run
    /// on), or is currently executing on a worker thread.
    TASK_RUNNING,
    /// Task is active, but waiting to run (either at a specific future time,
    /// or waiting for some event to wake it up.
    TASK_SNOOZED,
    /// Task is dead, will not run again.
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

class GlobalTask {
    friend class CompareByDueDate;
    friend class CompareByPriority;
    friend class CB3ExecutorPool;
public:

    GlobalTask(Taskable& t,
               TaskId taskId,
               double sleeptime = 0,
               bool completeBeforeShutdown = true);

    GlobalTask(EventuallyPersistentEngine *e,
               TaskId taskId,
               double sleeptime = 0,
               bool completeBeforeShutdown = true);

    virtual ~GlobalTask();

    /// execute the task and return true if it should be rescheduled
    bool execute();

    /**
     * Gives a description of this task.
     *
     * @return A description of this task
     */
    virtual std::string getDescription() const = 0;

    /**
     * The maximum expected duration of a single execution of this task - i.e.
     * how long should run() take.
     * Any task executions taking longer than this to run will be logged as
     * "slow".
     *
     * Exact values will vary significantly depending on the class of task;
     * however here are some general principles:
     *
     *   1. Our scheduler is non-preemptive, so a long-running task *cannot* be
     *      interrupted to allow another (possibly higher priority) task to run.
     *      As such, tasks in general should aim to only run for a brief
     *      duration at a time; for example at most 25ms (typical OS scheduler
     *      time-slice).
     *   2. Select a suitable limit for the given task - if a task is expected
     *      to complete in 1us; it isn't very useful to specify a limit of 25ms
     *      as we will fail to log any executions which are abnormally slow
     *      (even if they arn't causing scheduling issues for other tasks).
     *   3. Tasks which block other tasks while they run should aim to minimize
     *      their runtime - 25ms would be a significant delay to add to
     *      front-end operations if a particular task (e.g. HashTableResizer)
     *      blocks FE operations while running.
     *   4. One-off, startup tasks (e.g. Warmup) can take as long as necessary -
     *      given they must run before we can operate their duration isn't
     *      critical.
     */
    virtual std::chrono::microseconds maxExpectedDuration() const = 0;

    /**
     * test if a task is dead
     */
     bool isdead() {
        return (state == TASK_DEAD);
     }


    /**
     * Cancels this task by marking it dead.
     */
    void cancel() {
        state = TASK_DEAD;
    }

    /**
     * Puts the task to sleep for a given duration.
     */
    void snooze(const double secs);

    /**
     * Returns the id of this task.
     *
     * @return A unique task id number.
     */
    size_t getId() const { return uid; }

    /**
     * Returns the id of this task.
     *
     * @return The id of this task.
     */
    TaskId getTaskId() const {
        return taskId;
    }

    /**
     * Gets the engine that this task was scheduled from
     *
     * @returns A handle to the engine
     */
    EventuallyPersistentEngine* getEngine() { return engine; }

    task_state_t getState() {
        return state.load();
    }

    void setState(task_state_t tstate, task_state_t expected) {
        state.compare_exchange_strong(expected, tstate);
    }

    Taskable& getTaskable() const {
        return taskable;
    }

    std::chrono::steady_clock::time_point getWaketime() const {
        const auto waketime_chrono = std::chrono::nanoseconds(waketime);
        return std::chrono::steady_clock::time_point(waketime_chrono);
    }

    void updateWaketime(std::chrono::steady_clock::time_point tp) {
        waketime = to_ns_since_epoch(tp).count();
    }

    void updateWaketimeIfLessThan(std::chrono::steady_clock::time_point tp) {
        const int64_t tp_ns = to_ns_since_epoch(tp).count();
        atomic_setIfBigger(waketime, tp_ns);
    }

    std::chrono::steady_clock::time_point getLastStartTime() const {
        const auto waketime_chrono = std::chrono::nanoseconds(lastStartTime);
        return std::chrono::steady_clock::time_point(waketime_chrono);
    }

    void updateLastStartTime(std::chrono::steady_clock::time_point tp) {
        lastStartTime = to_ns_since_epoch(tp).count();
    }

    std::chrono::steady_clock::duration getTotalRuntime() const {
        return std::chrono::nanoseconds(totalRuntime);
    }

    std::chrono::steady_clock::duration getPrevRuntime() const {
        return std::chrono::nanoseconds(previousRuntime);
    }

    uint64_t getRunCount() const {
        return runCount;
    }

    void updateRuntime(std::chrono::steady_clock::duration tp) {
        int64_t nanoseconds =
                std::chrono::duration_cast<std::chrono::nanoseconds>(tp)
                        .count();
        lastStartTime = 0;
        totalRuntime += nanoseconds;
        previousRuntime = nanoseconds;
        runCount++;
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

    /**
     * If true then this Task blocks bucket shutdown - it must complete on it's
     * own accord and should not be cancelled.
     */
    const bool blockShutdown;

protected:
    /**
     * The invoked function when the task is executed.
     *
     * If the task wants to run again, it should return true - it will be
     * added back onto the ready queue and scheduled according to it's
     * priority. To run again but at a later time, call snooze() specifying
     * how long to sleep before it should be scheduled again.
     * If the task is complete (and should never be run again), return false.
     *
     * @return Whether or not this task should be rescheduled
     */
    virtual bool run() = 0;

    /**
     * Wake up a task, setting it's wakeTime to "now".
     *
     * Note: this is protected as this is only safe to call from a GlobalTasks'
     * own run() method; as it does not actually re-schedule the task itself
     * (after GlobalTask::run() completes CB3ExecutorThread re-checks wakeTime
     * and re-schedules as necessary).
     *
     * If you want to wake a task from outside it's own run() method; use
     * ExecutorPool::wake().
     */
    void wakeUp();

    /**
     * We are using a int64_t as opposed to ProcessTime::time_point because we
     * want the access to be atomic without the use of a mutex. The reason for
     * this is that we update these timepoints in locations which have been
     * shown to be pretty hot (e.g. CompareByDueDate) and we want to avoid
     * the overhead of acquiring mutexes.
     */
    using atomic_time_point = std::atomic<int64_t>;
    using atomic_duration = std::atomic<int64_t>;
    std::atomic<task_state_t> state;
    const size_t uid;
    const TaskId taskId;
    TaskPriority priority;
    EventuallyPersistentEngine *engine;
    Taskable& taskable;

    static std::atomic<size_t> task_id_counter;
    static size_t nextTaskId() { return task_id_counter.fetch_add(1); }

    atomic_duration totalRuntime;
    atomic_duration previousRuntime;
    /**
     * If the task is currently executing, the time it started. If the task
     * is not currently executing then zero.
     * Can be used to measure task runtime so far before task finishes.
     */
    atomic_time_point lastStartTime;
    /// How many times this task has been run.
    std::atomic<uint64_t> runCount{0};

private:
    atomic_time_point waketime; // used for priority_queue
};

typedef std::shared_ptr<GlobalTask> ExTask;

/**
 * Order tasks by their priority. If priority is the same, order by waketime to
 * ensure that we keep the ordering the tasks had when we moved them from the
 * futureQueue. This sort may not be stable, but if a task has the same priority
 * and wakeTime then we don't really care if they are re-ordered as wakeTime has
 * a nano second granularity.
 * @return true if t2 should have priority over t1
 */
class CompareByPriority {
public:
    bool operator()(ExTask &t1, ExTask &t2) {
        return (t1->priority == t2->priority)
                       ? (t1->waketime > t2->waketime)
                       : (t1->getQueuePriority() > t2->getQueuePriority());
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
