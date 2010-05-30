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
    if(pthread_create(&thread, NULL, launch_dispatcher_thread, this) != 0) {
        throw std::runtime_error("Error initializing dispatcher thread");
    }
}

void Dispatcher::run() {
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
                    std::cerr << "exception caught in task "
                              << task->name << ": " << e.what() << std::endl;
                } catch(...) {
                    std::cerr << "Caught a fatal exception in task"
                              << task->name <<std::endl;
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
    if (state == dispatcher_stopped) {
        return;
    }
    getLogger()->log(EXTENSION_LOG_DEBUG, NULL, "Stopping dispatcher\n");
    state = dispatcher_stopping;
    mutex.notify();
    lh.unlock();
    pthread_join(thread, NULL);
    getLogger()->log(EXTENSION_LOG_DEBUG, NULL, "Dispatcher stopped\n");
}

TaskId Dispatcher::schedule(shared_ptr<DispatcherCallback> callback, int priority,
                            double sleeptime) {
    getLogger()->log(EXTENSION_LOG_DEBUG, NULL, "Scheduling a new task\n");
    LockHolder lh(mutex);
    TaskId task(new Task(callback, priority, sleeptime));
    queue.push(task);
    mutex.notify();
    return TaskId(task);
}

TaskId Dispatcher::wake(TaskId task) {
    cancel(task);
    TaskId oldTask(task);
    TaskId newTask(new Task(*oldTask));
    queue.push(newTask);
    mutex.notify();
    return TaskId(newTask);
}
