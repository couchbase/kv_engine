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

/*
 * FakeExecutorPool / FakeExecutorThread
 *
 * A pair of classes which act as a fake ExecutorPool for testing purposes.
 * Only executes tasks when explicitly told, and only on the main thread.
 *
 * See SingleThreadedEPStoreTest for basic usage.
 *
 * TODO: Improve usage documentation.
 */

#pragma once

#include "cb3_executorpool.h"
#include "cb3_executorthread.h"
#include "objectregistry.h"
#include "taskqueue.h"

#include <folly/portability/GTest.h>

class SingleThreadedExecutorPool : public CB3ExecutorPool {
public:

    /* Registers an instance of this class as "the" executorpool (i.e. what
     * you get when you call ExecutorPool::get()).
     *
     * This *must* be called before the normal ExecutorPool is created.
     */
    static void replaceExecutorPoolWithFake() {
        LockHolder lh(initGuard);
        auto* tmp = instance.load();
        if (tmp != nullptr) {
            throw std::runtime_error("replaceExecutorPoolWithFake: "
                    "ExecutorPool instance already created - cowardly refusing to continue!");
        }

        EventuallyPersistentEngine *epe =
                ObjectRegistry::onSwitchThread(nullptr, true);
        tmp = new SingleThreadedExecutorPool();
        ObjectRegistry::onSwitchThread(epe);
        instance.store(tmp);
    }

    explicit SingleThreadedExecutorPool()
        : CB3ExecutorPool(/*threads*/ 0,
                          ThreadPoolConfig::ThreadCount::Default,
                          ThreadPoolConfig::ThreadCount::Default,
                          0,
                          0) {
    }

    bool _startWorkers() override {
        // Don't actually start any worker threads (all work will be done
        // synchronously in the same thread)
        return true;
    }

    // Helper methods to access normally protected state of ExecutorPool

    TaskQ& getLpTaskQ() {
        return lpTaskQ;
    }

    /*
     * Mark all tasks as cancelled and remove the from the locator.
     */
    void cancelAndClearAll() {
        LockHolder lh(tMutex);
        cancelAll_UNLOCKED();
        taskLocator.clear();
    }

       /*
     * Mark all tasks as cancelled and remove the from the locator.
     */
    void cancelAll() {
        LockHolder lh(tMutex);
        cancelAll_UNLOCKED();
    }

    /*
     * Cancel all tasks with a matching name
     */
    void cancelByName(std::string_view name) {
        LockHolder lh(tMutex);
        for (auto& it : taskLocator) {
            if (name == it.second.first->getDescription().c_str()) {
                it.second.first->cancel();
                // And force awake so he is "runnable"
                it.second.second->wake(it.second.first);
            }
        }
    }

    /*
     * Check if task with given name exists
     */
    bool isTaskScheduled(task_type_t queueType, std::string_view name) {
        LockHolder lh(tMutex);
        for (auto& it : taskLocator) {
            auto description = it.second.first->getDescription();
            if (name != std::string_view(description.c_str())) {
                continue;
            }
            if (it.second.second->getQueueType() != queueType) {
                continue;
            }
            return true;
        }
        return false;
    }

    size_t getTotReadyTasks() {
        return totReadyTasks;
    }

    size_t getNumReadyTasksOfType(task_type_t qType) {
        return numReadyTasks[qType];
    }

    std::map<size_t, TaskQpair> getTaskLocator() {
        return taskLocator;
    };

    /**
     * Runs the next task with the expected name from the given task queue.
     * The task is run synchronously in the current thread.
     */
    void runNextTask(task_type_t taskType, std::string expectedTask);

private:
    void cancelAll_UNLOCKED() {
        for (auto& it : taskLocator) {
            it.second.first->cancel();
            // And force awake so he is "runnable"
            it.second.second->wake(it.second.first);
        }
    }
};

/*
 * A container for a single task to 'execute' on divorced of the logical thread.
 * Performs checks of the taskQueue once execution is complete.
 */
class CheckedExecutor : public CB3ExecutorThread {
public:
    CheckedExecutor(ExecutorPool* manager_, TaskQueue& q)
        // TODO: The dynamic_cast will fail if the ExecutorPool is
        // not CB3ExecutorPool (e.g. FollyExecutorPool).
        : CB3ExecutorThread(dynamic_cast<CB3ExecutorPool*>(manager_),
                            q.getQueueType(),
                            "checked_executor"),
          queue(q),
          preFutureQueueSize(queue.getFutureQueueSize()),
          preReadyQueueSize(queue.getReadyQueueSize()),
          rescheduled(false) {
        if (!queue.fetchNextTask(*this)) {
            throw std::logic_error("CheckedExecutor failed fetchNextTask");
        }

        // Configure a checker to run, some tasks are subtly different
        if (getTaskName() == "Snapshotting vbucket states" ||
            getTaskName() == "Removing closed unreferenced checkpoints from memory" ||
            getTaskName() == "Paging expired items." ||
            getTaskName() == "Adjusting hash table sizes." ||
            getTaskName() == "Generating access log") {
            checker = [=](bool taskRescheduled) {
                // These tasks all schedule one other task
                this->oneExecutes(taskRescheduled, 1);
            };
        } else if (getTaskName() == "Paging out items.") {
            checker = [=](bool taskRescheduled) {
                // This task _may_ schedule a single task.
                this->oneExecutes(taskRescheduled, /*min*/ 0, /*max*/ 1);
            };
        } else {
            checker = [=](bool taskRescheduled) {
                this->oneExecutes(taskRescheduled, 0);
            };
        }
    }

    void runCurrentTask(std::string_view expectedTask) {
        EXPECT_EQ(expectedTask, getTaskName());
        run();
    }

    void runCurrentTask() {
        run();
    }

    std::chrono::steady_clock::time_point completeCurrentTask() {
        auto min_waketime = std::chrono::steady_clock::time_point::min();
        manager->doneWork(taskType);
        if (rescheduled && !currentTask->isdead()) {
            min_waketime = queue.reschedule(currentTask);
        } else {
            manager->cancel(currentTask->getId(), true);
        }

        if (!currentTask->isdead()) {
            checker(rescheduled);
        }
        return min_waketime;
    }

    ExTask& getCurrentTask() {
        return currentTask;
    }

private:

    /*
     * Performs checks based on the assumption that one task executes and can
     * as part of that execution
     *   - request itself to be rescheduled
     *   - schedule other tasks (expectedToBeScheduled)
     */
    void oneExecutes(bool rescheduled,
                     int minExpectedToBeScheduled,
                     int maxExpectedToBeScheduled) {
        auto expected = preFutureQueueSize + preReadyQueueSize;
        auto actual = queue.getFutureQueueSize() + queue.getReadyQueueSize();

        if (!rescheduled) {
            // the task did _not_ reschedule itself, so expect one fewer task
            expected--;
        }

        // Check that the new sizes of the future and ready tally given
        // one executed and n were scheduled as a side effect.

        // there should now be _at least_ minExpectedToBeScheduled extra
        // tasks
        EXPECT_GE(actual, expected + minExpectedToBeScheduled);
        // there should now be _no more than_ maxExpectedToBeScheduled extra
        // tasks
        EXPECT_LE(actual, expected + maxExpectedToBeScheduled);
    }

    void oneExecutes(bool rescheduled, int expectedToBeScheduled) {
        // expect _exactly_ the specified number of tasks to be scheduled
        oneExecutes(rescheduled,
                    /*min*/ expectedToBeScheduled,
                    /*max*/ expectedToBeScheduled);
    }

    /*
     * Run the task and record if it was rescheduled.
     */
    void run() {
        if (currentTask->isdead()) {
            rescheduled = false;
            return;
        }
        rescheduled = currentTask->execute();
    }

    TaskQueue& queue;
    size_t preFutureQueueSize;
    size_t preReadyQueueSize;
    bool rescheduled;

    /*
     * A function object that runs post task execution for the purpose of
     * running checks against state changes.
     * The defined function accepts one boolean parameter that states if the
     * task which just executed has been rescheduled.
     */
    std::function<void(bool)> checker;
};

inline void SingleThreadedExecutorPool::runNextTask(task_type_t taskType,
                                                    std::string expectedTask) {
    CheckedExecutor executor(this, *getLpTaskQ()[taskType]);
    executor.runCurrentTask(expectedTask);
    executor.completeCurrentTask();
}
