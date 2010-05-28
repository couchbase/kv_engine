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

    TaskId schedule(shared_ptr<DispatcherCallback> callback, int priority=0,
                    double sleeptime=0) {
        getLogger()->log(EXTENSION_LOG_DEBUG, NULL, "Scheduling a new task\n");
        LockHolder lh(mutex);
        TaskId task(new Task(callback, priority, sleeptime));
        queue.push(task);
        mutex.notify();
        return TaskId(task);
    }

    TaskId wake(TaskId task) {
        cancel(task);
        TaskId oldTask(task);
        TaskId newTask(new Task(*oldTask));
        queue.push(newTask);
        mutex.notify();
        return TaskId(newTask);
    }

    void start();

    void run() {
        getLogger()->log(EXTENSION_LOG_DEBUG, NULL, "Dispatcher starting\n");
        while (state == dispatcher_running) {
            LockHolder lh(mutex);
            if (queue.empty()) {
                // Wait forever
                mutex.wait();
            } else {
                TaskId task = queue.top();
                assert(task);
                LockHolder tlh(task->mutex);
                switch (task->state) {
                case task_sleeping:
                    {
                        struct timeval tv;
                        gettimeofday(&tv, NULL);
                        if (less_tv(tv, task->waketime)) {
                            tlh.unlock();
                            mutex.wait(task->waketime);
                        } else {
                            task->state = task_running;
                        }
                    }
                    break;
                case task_running:
                    queue.pop();
                    lh.unlock();
                    tlh.unlock();
                    try {
                        if(task->run(*this, TaskId(task))) {
                            reschedule(task);
                        }
                    } catch (std::exception& e) {
                        std::cerr << "exception caught in task " << task->name << ": " << e.what() << std::endl;
                    } catch(...) {
                        std::cerr << "Caught a fatal exception in task" << task->name <<std::endl;
                    }
                    break;
                case task_dead:
                    queue.pop();
                    break;
                default:
                    throw std::runtime_error("Unexpected state for task");
                }
            }
        }

        state = dispatcher_stopped;
        mutex.notify();
        getLogger()->log(EXTENSION_LOG_DEBUG, NULL, "Dispatcher exited\n");
    }

    void stop() {
        LockHolder lh(mutex);
        if (state == dispatcher_stopped || state == dispatcher_stopping) {
            return;
        }
        getLogger()->log(EXTENSION_LOG_DEBUG, NULL, "Stopping dispatcher\n");
        state = dispatcher_stopping;
        mutex.notify();
        lh.unlock();
        pthread_join(thread, NULL);
        getLogger()->log(EXTENSION_LOG_DEBUG, NULL, "Dispatcher stopped\n");
    }

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
