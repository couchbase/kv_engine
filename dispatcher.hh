/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#ifndef DISPATCHER_HH
#define DISPATCHER_HH

#include <stdexcept>
#include <queue>

#include "common.hh"
#include "locks.hh"

class Dispatcher;

enum task_state {
    task_dead,
    task_running,
    task_sleeping
};

enum dispatcher_state {
    dispatcher_running,
    dispatcher_stopping,
    dispatcher_stopped
};

class Task;

typedef shared_ptr<Task> TaskId;

class DispatcherCallback {
public:
    virtual ~DispatcherCallback() {}
    virtual bool callback(Dispatcher &d, TaskId t) = 0;
};

class CompareTasks;

class Task {
friend class CompareTasks;
public:
    ~Task() { }
private:
    Task(shared_ptr<DispatcherCallback> cb, int p=0, double sleeptime=0) :
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

class Dispatcher {
public:
    Dispatcher() : state(dispatcher_running) { }

    ~Dispatcher() {
        stop();
    }

    void schedule(shared_ptr<DispatcherCallback> callback,
                  TaskId *outtid,
                  int priority=0, double sleeptime=0);

    void wake(TaskId task, TaskId *outtid);

    void start();
    void stop();

    void run();

    void snooze(TaskId t, double sleeptime) {
        t->snooze(sleeptime);
    }

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
