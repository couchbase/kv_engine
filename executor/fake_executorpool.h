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
#include "cb3_taskqueue.h"

#include <engines/ep/src/objectregistry.h>
#include <memory>

class SingleThreadedExecutorPool : public CB3ExecutorPool {
public:
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
        std::lock_guard<std::mutex> lh(tMutex);
        cancelAll_UNLOCKED();
        taskLocator.clear();
    }

    /*
     * Mark all tasks as cancelled and remove the from the locator.
     */
    void cancelAll() {
        std::lock_guard<std::mutex> lh(tMutex);
        cancelAll_UNLOCKED();
    }

    /*
     * Cancel all tasks with a matching name
     */
    void cancelByName(std::string_view name) {
        std::lock_guard<std::mutex> lh(tMutex);
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
        std::lock_guard<std::mutex> lh(tMutex);
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
            getTaskName() == "Adjusting hash table sizes." ||
            getTaskName() == "Generating access log") {
            checker = [=](bool taskRescheduled) {
                // These tasks all schedule one other task
                this->oneExecutes(taskRescheduled, 1);
            };
        } else if (getTaskName() == "CheckpointMemRecoveryTask:0") {
            checker = [=](bool taskRescheduled) {
                // This task _may_ schedule or wake a CheckpointDestroyer, to
                // destroy checkpoints made unreferenced by cursor dropping
                this->oneExecutes(taskRescheduled, /*min*/ 0, /*max*/ 2);
            };
        } else if (getTaskName() == "Paging out items." ||
                   getTaskName() == "Paging expired items.") {
            checker = [=](bool taskRescheduled) {
                // This task _may_ schedule N subsequent tasks.
                // Bound it at 10 as a sanity check. If tests wish to configure
                // more than 10 concurrent visitors, this can be bumped.
                this->oneExecutes(taskRescheduled, /*min*/ 0, /*max*/ 10);
            };
        } else {
            checker = [=](bool taskRescheduled) {
                this->oneExecutes(taskRescheduled, 0);
            };
        }
    }

    void runCurrentTask(std::string_view expectedTask) {
        if (expectedTask != getTaskName()) {
            std::string message =
                    "CheckedExecutor::runCurrentTask(): Expected task: \"";
            message.append(expectedTask);
            message.append("\" got \"");
            message.append(getTaskName());
            message.append("\"");
            throw std::runtime_error(message);
        }
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

        if (actual < expected + minExpectedToBeScheduled) {
            throw std::runtime_error(
                    "CheckedExecutor::oneExecutes(): Expected " +
                    std::to_string(actual) + " >= " +
                    std::to_string(expected + minExpectedToBeScheduled));
        }

        // there should now be _no more than_ maxExpectedToBeScheduled extra
        // tasks
        if (actual > expected + maxExpectedToBeScheduled) {
            throw std::runtime_error(
                    "CheckedExecutor::oneExecutes(): Expected " +
                    std::to_string(actual) + " <= " +
                    std::to_string(expected + maxExpectedToBeScheduled));
        }
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
        rescheduled = currentTask->execute(getName());
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
