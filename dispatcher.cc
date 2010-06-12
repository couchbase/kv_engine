#include "dispatcher.hh"

extern "C" {
    static void* launch_dispatcher_thread(void* arg);
}

static void* launch_dispatcher_thread(void *arg) {
    Dispatcher *dispatcher = (Dispatcher*) arg;
    try {
        dispatcher->run();
    } catch (std::exception& e) {
        getLogger()->log(EXTENSION_LOG_WARNING, NULL, "dispatcher exception caught: %s\n",
                         e.what());
    } catch(...) {
        getLogger()->log(EXTENSION_LOG_WARNING, NULL,
                         "Caught a fatal exception in the dispatcher thread\n");
    }
    return NULL;
}

void Dispatcher::start() {
    assert(state == dispatcher_running);
    if(pthread_create(&thread, NULL, launch_dispatcher_thread, this) != 0) {
        throw std::runtime_error("Error initializing dispatcher thread");
    }
}

void Dispatcher::run() {
    getLogger()->log(EXTENSION_LOG_DEBUG, NULL, "Dispatcher starting\n");
    while (state == dispatcher_running) {
        LockHolder lh(mutex);
        if (queue.empty()) {
            // Wait forever as long as the state didn't change while
            // we grabbed the lock.
            if (state == dispatcher_running) {
                mutex.wait();
            }
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
                    getLogger()->log(EXTENSION_LOG_WARNING, NULL, "exception caught in task %s: %s\n", task->name.c_str(),
                                     e.what());
                } catch(...) {
                    getLogger()->log(EXTENSION_LOG_WARNING, NULL, "fatal exception caught in task %s\n", task->name.c_str());
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

void Dispatcher::stop() {
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

void Dispatcher::schedule(shared_ptr<DispatcherCallback> callback,
                          TaskId *outtid,
                          int priority, double sleeptime) {
    getLogger()->log(EXTENSION_LOG_DEBUG, NULL, "Scheduling a new task\n");
    LockHolder lh(mutex);
    TaskId task(new Task(callback, priority, sleeptime));
    if (outtid) {
        *outtid = TaskId(task);
    }
    queue.push(task);
    mutex.notify();
}

void Dispatcher::wake(TaskId task, TaskId *outtid) {
    cancel(task);
    LockHolder lh(mutex);
    TaskId oldTask(task);
    TaskId newTask(new Task(*oldTask));
    if (outtid) {
        *outtid = TaskId(task);
    }
    queue.push(newTask);
    mutex.notify();
}
