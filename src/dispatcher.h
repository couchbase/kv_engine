/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2010 Couchbase, Inc
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

#ifndef SRC_DISPATCHER_H_
#define SRC_DISPATCHER_H_ 1

#include "config.h"

#include <deque>
#include <queue>
#include <stdexcept>
#include <string>
#include <vector>

#include "atomic.h"
#include "common.h"
#include "locks.h"
#include "priority.h"
#include "ringbuffer.h"

#define JOB_LOG_SIZE 20

class Dispatcher;

/**
 * States a task may be in.
 */
enum task_state {
    task_dead,                  //!< The task is dead and should not be executed
    task_running                //!< The task is running
};

/**
 * States the dispatcher may be in.
 */
enum dispatcher_state {
    dispatcher_running,         //!< The dispatcher is running
    dispatcher_stopping,        //!< The dispatcher is shutting down
    dispatcher_stopped          //!< The dispatcher has shut down
};

class EventuallyPersistentEngine;

/**
 * Log entry for previous job runs.
 */
class JobLogEntry {
public:

    // This is useful for the ringbuffer to initialize
    JobLogEntry() : name("invalid"), duration(0) {}
    JobLogEntry(const std::string &n, const hrtime_t d, rel_time_t t = 0)
        : name(n), ts(t), duration(d) {}

    /**
     * Get the name of the job.
     */
    std::string getName() const { return name; }

    /**
     * Get the amount of time (in microseconds) this job ran.
     */
    hrtime_t getDuration() const { return duration; }

    /**
     * Get a timestamp indicating when this thing started.
     */
    rel_time_t getTimestamp() const { return ts; }

private:
    std::string name;
    rel_time_t ts;
    hrtime_t duration;
};

class Task;

typedef SingleThreadedRCPtr<Task> TaskId;

/**
 * Code executed when the dispatcher is ready to do your work.
 */
class DispatcherCallback {
public:
    virtual ~DispatcherCallback() {}
    /**
     * Perform my task.
     *
     * @param d the dispatcher running this task
     * @param t the task
     *
     * @return true if the task should run again
     */
    virtual bool callback(Dispatcher &d, TaskId &t) = 0;

    //! Print a human-readable description of this callback.
    virtual std::string description() = 0;

    /**
     * Maximum amount of time (in microseconds) this job should run
     * before considered slow.
     */
    virtual hrtime_t maxExpectedDuration() {
        // Default == 1 second
        return 1 * 1000 * 1000;
    }

    /**
     *! Dispatcher callback task start time at GMT hour (0 to 23)
     */
    virtual size_t startTime() {
        return 24;
    }
};

class CompareTasksByDueDate;
class CompareTasksByPriority;

/**
 * Tasks managed by the dispatcher.
 */
class Task : public RCValue {
friend class CompareTasksByDueDate;
friend class CompareTasksByPriority;
public:
    virtual ~Task() { }

    const struct timeval& getWaketime() const { return waketime; }

protected:
    Task(shared_ptr<DispatcherCallback> cb,  int p, double sleeptime = 0,
         bool isDaemon = true, bool completeBeforeShutdown = false) :
        RCValue(), callback(cb), priority(p),
        state(task_running), isDaemonTask(isDaemon),
        blockShutdown(completeBeforeShutdown)
    {
        snooze(sleeptime, true);
    }

    void snooze(const double secs, bool first=false);

    virtual bool run(Dispatcher &d, TaskId &t) {
        return callback->callback(d, t);
    }

    void cancel() {
        LockHolder lh(mutex);
        state = task_dead;
    }

    virtual std::string getName() {
        return callback->description();
    }

    virtual hrtime_t maxExpectedDuration() {
        return callback->maxExpectedDuration();
    }

    friend class Dispatcher;
    struct timeval waketime;
    shared_ptr<DispatcherCallback> callback;
    int priority;
    enum task_state state;
    Mutex mutex;
    bool isDaemonTask;

    // Some of the tasks must complete during shutdown
    bool blockShutdown;

private:
    DISALLOW_COPY_AND_ASSIGN(Task);
};

/**
 * Internal task run by the dispatcher when it wants to sleep.
 */
class IdleTask : public Task {
public:

    IdleTask() : Task(shared_ptr<DispatcherCallback>(), 0),
                 dnotifications(0) {}

    bool run(Dispatcher &d, TaskId &t);

    std::string getName() {
        return std::string("IdleTask (sleeping)");
    }

    /**
     * Set the next waketime.
     */
    void setWaketime(struct timeval to) {
        waketime = to;
    }

    /**
     * Set the number of items enqueued for this dispatcher at the
     * time of execution prep.
     */
    void setDispatcherNotifications(size_t to) {
        dnotifications = to;
    }

    hrtime_t maxExpectedDuration() {
        hrtime_t rv(3600);
        rv *= (1000 * 1000);
        return rv;
    }

private:
    size_t dnotifications;
    DISALLOW_COPY_AND_ASSIGN(IdleTask);
};

/**
 * Order tasks by their ready date.
 */
class CompareTasksByDueDate {
public:
    // true if t1 is before t2
    bool operator()(TaskId &t1, TaskId &t2) {
        return less_tv(t2->waketime, t1->waketime);
    }
};

/**
 * Order tasks by their priority.
 */
class CompareTasksByPriority {
public:
    bool operator()(TaskId &t1, TaskId &t2) {
        return t1->priority > t2->priority;
    }
};

/**
 * Snapshot of the state of a dispatcher.
 */
class DispatcherState {
public:
    DispatcherState(const std::string &name,
                    enum dispatcher_state st,
                    hrtime_t start, bool running,
                    std::vector<JobLogEntry> jl,
                    std::vector<JobLogEntry> sj)
        : joblog(jl), slowjobs(sj), taskName(name),
          state(st), taskStart(start), running_task(running) {}

    /**
     * Get the name of the current dispatcher state.
     */
    const char* getStateName() const {
        const char *rv = NULL;
        switch(state) {
        case dispatcher_stopped: rv = "dispatcher_stopped"; break;
        case dispatcher_running: rv = "dispatcher_running"; break;
        case dispatcher_stopping: rv = "dispatcher_stopping"; break;
        }
        return rv;
    }

    /**
     * Get the time the current task started.
     */
    hrtime_t getTaskStart() const { return taskStart; }

    /**
     * Get the name of the currently running task.
     */
    const std::string getTaskName() const { return taskName; }

    /**
     * True if the dispatcher is currently running a task.
     */
    bool isRunningTask() const { return running_task; }

    /**
     * Retrieve the log of recently completed jobs.
     */
    const std::vector<JobLogEntry> getLog() const { return joblog; }

    /**
     * Retrieve the log of recently completed slow jobs.
     */
    const std::vector<JobLogEntry> getSlowLog() const { return slowjobs; }

private:
    const std::vector<JobLogEntry> joblog;
    const std::vector<JobLogEntry> slowjobs;
    const std::string taskName;
    const enum dispatcher_state state;
    const hrtime_t taskStart;
    const bool running_task;
};

/**
 * Schedule and run tasks in another thread.
 */
class Dispatcher {
public:
    Dispatcher(EventuallyPersistentEngine &e, const char *desc = NULL) :
        notifications(0), joblog(JOB_LOG_SIZE), slowjobs(JOB_LOG_SIZE),
        idleTask(new IdleTask), state(dispatcher_running), running_task(false),
        hasWokenTask(false), forceTermination(false), engine(e),
        name(desc ? desc : "Dispatcher")
    {
        noTask();
    }

    ~Dispatcher() {
        stop();
    }

    /**
     * Schedule a job to run.
     *
     * @param callback a shared_ptr to the callback to run
     * @param outtid an output variable that will receive the task ID (may be NULL)
     * @param priority job priority instance that defines a job's priority
     * @param sleeptime how long (in seconds) to wait before starting the job
     * @param isDaemon a flag indicating if a task is daemon or not
     * @param mustComplete set to true if this task must complete before the
     *        dispatcher can shut down
     */
    void schedule(shared_ptr<DispatcherCallback> callback,
                  TaskId *outtid,
                  const Priority &priority,
                  double sleeptime = 0,
                  bool isDaemon = true,
                  bool mustComplete = false);

    /**
     * Wake up the given task.
     *
     * @param task the task to wake up
     */
    void wake(TaskId &task);

    /**
     * Start this dispatcher's thread.
     */
    void start();
    /**
     * Stop this dispatcher.
     * @param force the flag indicating the force termination or not.
     */
    void stop(bool force = false);

    /**
     * Dispatcher's main loop.  Don't run this.
     */
    void run();

    /**
     * Delay a task.
     *
     * @param t the task to delay
     * @param sleeptime how long to delay the task
     */
    void snooze(TaskId &t, double sleeptime);

    /**
     * Cancel a task.
     */
    void cancel(TaskId &t);

    /**
     * Get the name of the currently executing task.
     */
    std::string getCurrentTaskName() { return taskDesc; }

    /**
     * Get the state of the dispatcher.
     */
    enum dispatcher_state getState() { return state; }

    DispatcherState getDispatcherState() {
        LockHolder lh(mutex);
        return DispatcherState(taskDesc, state, taskStart, running_task,
                               joblog.contents(), slowjobs.contents());
    }

    const std::string &getName() { return name; }

private:

    friend class IdleTask;

    void noTask() {
        taskDesc = "none";
    }

    void reschedule(TaskId &task);

    void notify() {
        ++notifications;
        mutex.notify();
    }

    /**
     * Complete all the non-daemon tasks before stopping the dispatcher
     */
    void completeNonDaemonTasks();

    /**
     * Move all tasks that are ready for execution into the "ready"
     * priority queue.
     */
    void moveReadyTasks(const struct timeval &tv);

    //! True if there are no tasks scheduled.
    bool empty() { return readyQueue.empty() && futureQueue.empty(); }

    //! Get the next task.
    TaskId nextTask();

    //! Remove the next task.
    void popNext();

    std::string taskDesc;
    cb_thread_t thread;
    SyncObject mutex;
    Atomic<size_t> notifications;
    std::priority_queue<TaskId, std::deque<TaskId >,
                        CompareTasksByPriority> readyQueue;
    std::priority_queue<TaskId, std::deque<TaskId >,
                        CompareTasksByDueDate> futureQueue;
    RingBuffer<JobLogEntry> joblog;
    RingBuffer<JobLogEntry> slowjobs;
    SingleThreadedRCPtr<IdleTask> idleTask;
    enum dispatcher_state state;
    hrtime_t taskStart;
    bool running_task;
    bool hasWokenTask;
    bool forceTermination;

    EventuallyPersistentEngine &engine;
    std::string name;
};

#endif  // SRC_DISPATCHER_H_
