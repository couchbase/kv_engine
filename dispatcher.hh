/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#ifndef DISPATCHER_HH
#define DISPATCHER_HH

#include <stdexcept>
#include <queue>

#include "common.hh"
#include "locks.hh"
#include "priority.hh"

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

class Task;

typedef shared_ptr<Task> TaskId;

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
    virtual bool callback(Dispatcher &d, TaskId t) = 0;

    //! Print a human-readable description of this callback.
    virtual std::string description() = 0;
};

class CompareTasksByDueDate;
class CompareTasksByPriority;

/**
 * Tasks managed by the dispatcher.
 */
class Task {
friend class CompareTasksByDueDate;
friend class CompareTasksByPriority;
public:
    ~Task() { }
private:
    Task(shared_ptr<DispatcherCallback> cb,  int p, double sleeptime=0, bool isDaemon=true) :
        name(cb->description()), callback(cb), priority(p),
        state(task_running), isDaemonTask(isDaemon) {
        snooze(sleeptime);
    }

    Task(const Task &task) {
        priority = task.priority;
        state = task_running;
        callback = task.callback;
        isDaemonTask = task.isDaemonTask;
    }

    void snooze(const double secs) {
        LockHolder lh(mutex);
        gettimeofday(&waketime, NULL);
        advance_tv(waketime, secs);
    }

    bool run(Dispatcher &d, TaskId t) {
        return callback->callback(d, t);
    }

    void cancel() {
        LockHolder lh(mutex);
        state = task_dead;
    }

    friend class Dispatcher;
    std::string name;
    struct timeval waketime;
    shared_ptr<DispatcherCallback> callback;
    int priority;
    enum task_state state;
    Mutex mutex;
    bool isDaemonTask;
};

/**
 * Order tasks by their ready date.
 */
class CompareTasksByDueDate {
public:
    // true if t1 is before t2
    bool operator()(TaskId t1, TaskId t2) {
        return less_tv(t2->waketime, t1->waketime);
    }
};

/**
 * Order tasks by their priority.
 */
class CompareTasksByPriority {
public:
    bool operator()(TaskId t1, TaskId t2) {
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
                    hrtime_t start, bool running)
        : taskName(name), state(st), taskStart(start),
          running_task(running) {}

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

private:
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
    Dispatcher() : state(dispatcher_running) {
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
     */
    void schedule(shared_ptr<DispatcherCallback> callback,
                  TaskId *outtid,
                  const Priority &priority, double sleeptime=0, bool isDaemon=true);

    /**
     * Wake up the given task.
     *
     * @param task the task to wake up
     * @param outtid a newly assigned task ID (may be NULL)
     */
    void wake(TaskId task, TaskId *outtid);

    /**
     * Start this dispatcher's thread.
     */
    void start();
    /**
     * Stop this dispatcher.
     */
    void stop();

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
    void snooze(TaskId t, double sleeptime) {
        t->snooze(sleeptime);
    }

    /**
     * Cancel a task.
     */
    void cancel(TaskId t) {
        t->cancel();
    }

    /**
     * Get the name of the currently executing task.
     */
    std::string getCurrentTaskName() { return taskDesc; }

    /**
     * Get the state of the dispatcher.
     */
    enum dispatcher_state getState() { return state; }

    DispatcherState getDispatcherState() {
        return DispatcherState(taskDesc, state, taskStart, running_task);
    }

private:

    void noTask() {
        taskDesc = "none";
    }

    void reschedule(TaskId task) {
        // If the task is already in the queue it'll get run twice
        LockHolder lh(mutex);
        futureQueue.push(task);
    }

    /**
     * Complete all the non-daemon tasks before stopping the dispatcher
     */
    void completeNonDaemonTasks();

    /**
     * Move all tasks that are ready for execution into the "ready"
     * priority queue.
     */
    void moveReadyTasks();

    //! True if there are no tasks scheduled.
    bool empty() { return readyQueue.empty() && futureQueue.empty(); }

    //! Get the next task.
    TaskId nextTask();

    //! Remove the next task.
    void popNext();

    std::string taskDesc;
    pthread_t thread;
    SyncObject mutex;
    std::priority_queue<TaskId, std::deque<TaskId >,
                        CompareTasksByPriority> readyQueue;
    std::priority_queue<TaskId, std::deque<TaskId >,
                        CompareTasksByDueDate> futureQueue;
    enum dispatcher_state state;
    hrtime_t taskStart;
    bool running_task;
};

#endif
