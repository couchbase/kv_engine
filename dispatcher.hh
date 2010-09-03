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
    task_running,               //!< The task is running
    task_sleeping               //!< The task will run later
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
};

class CompareTasks;

/**
 * Tasks managed by the dispatcher.
 */
class Task {
friend class CompareTasks;
public:
    ~Task() { }
private:
    Task(shared_ptr<DispatcherCallback> cb,  int p, double sleeptime=0) :
         callback(cb), priority(p) {
        if (sleeptime > 0) {
            snooze(sleeptime);
        } else {
            state = task_running;
        }
    }

    Task(const Task &task) {
        priority = task.priority;
        state = task_running;
        callback = task.callback;
    }

    void snooze(const double secs) {
        LockHolder lh(mutex);
        gettimeofday(&waketime, NULL);
        advance_tv(waketime, secs);
        state = task_sleeping;
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
};

/**
 * Order tasks into their natural execution order.
 */
class CompareTasks {
public:
    bool operator()(TaskId t1, TaskId t2) {
        if (t1->state == task_running) {
            if (t2->state == task_running) {
                return t1->priority > t2->priority;
            } else if (t2->state == task_sleeping) {
                return false;
            }
        } else if (t1->state == task_sleeping && t2->state == task_sleeping) {
            return less_tv(t2->waketime, t1->waketime);
        }
        return true;
    }
};

/**
 * Schedule and run tasks in another thread.
 */
class Dispatcher {
public:
    Dispatcher() : state(dispatcher_running) { }

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
     */
    void schedule(shared_ptr<DispatcherCallback> callback,
                  TaskId *outtid,
                  const Priority &priority, double sleeptime=0);

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

private:

    void reschedule(TaskId task) {
        // If the task is already in the queue it'll get run twice
        LockHolder lh(mutex);
        queue.push(task);
    }

    pthread_t thread;
    SyncObject mutex;
    std::priority_queue<TaskId, std::deque<TaskId >,
                        CompareTasks> queue;
    enum dispatcher_state state;
};

#endif
