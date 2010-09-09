/*
 *     Copyright 2010 NorthScale, Inc.
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
#include "config.h"
#include "dispatcher.hh"

extern "C" {
    static void* launch_dispatcher_thread(void* arg);
}

static void* launch_dispatcher_thread(void *arg) {
    Dispatcher *dispatcher = (Dispatcher*) arg;
    try {
        dispatcher->run();
    } catch (std::exception& e) {
        getLogger()->log(EXTENSION_LOG_WARNING, NULL,
                         "dispatcher exception caught: %s\n", e.what());
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
    for (;;) {
        LockHolder lh(mutex);
        // Having acquired the lock, verify our state and break out if
        // it's changed.
        if (state != dispatcher_running) {
            break;
        }
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
                    getLogger()->log(EXTENSION_LOG_WARNING, NULL,
                                     "exception caught in task %s: %s\n",
                                     task->name.c_str(), e.what());
                } catch(...) {
                    getLogger()->log(EXTENSION_LOG_WARNING, NULL,
                                     "fatal exception caught in task %s\n",
                                     task->name.c_str());
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
                          const Priority &priority, double sleeptime) {
    LockHolder lh(mutex);
    TaskId task(new Task(callback, priority.getPriorityValue(), sleeptime));
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
