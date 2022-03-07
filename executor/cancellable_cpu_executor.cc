/*
 *     Copyright 2022-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#include "cancellable_cpu_executor.h"

#include <logger/logger.h>

void CancellableCPUExecutor::add(GlobalTask* task, folly::Func func) {
    tasks.enqueue(QueueElem(task, std::move(func)));

    cpuPool.add([this]() {
        // Here we are trying to orchestrate consistency between two
        // folly::UMPMC queues which both push/pop items with a release
        // acquire level of synchronization. That's fine if you want to
        // ensure the consistency of each variable individually but we
        // require a greater level of consistency (sequential) to ensure
        // that when we pop something from the folly queue the associated
        // item exists in our queue. To accomplish this we'll use a memory
        // fence before we read from our queue to ensure that it is
        // up to date. Were the scheduler thread to run and add something
        // new to our queue after the fence then that would be fine as it
        // would mean that a new task is available (and a thread will be
        // woken if not busy to run it). The scheduler thread may remove
        // a task if we are unregistering a taskable, but that's fine too as
        // that requires a weaker level of consistency (release/acquire) as
        // it only modifies that tasks queue and not the cpuPool queue.
        std::atomic_thread_fence(std::memory_order_seq_cst);
        auto task = tasks.try_dequeue();

        // No guarantees that we find a task here, we may have left
        // tasks in the cpuPool queue to wake but removed them if we
        // have unregistered a taskable. We should always have more or the
        // same amount of tasks (notifications) in the cpuPool as our queue
        // so just try_dequeue
        if (task) {
            task->func();
        }
    });
}

std::vector<GlobalTask*> CancellableCPUExecutor::removeTasksForTaskable(
        const Taskable& taskable) {
    std::vector<QueueElem> tasksToPushBack;
    std::vector<QueueElem> tasksToRun;
    std::vector<GlobalTask*> tasksToCancel;

    // Pop everything into our three vectors as appropriate so that we can
    // cancel the tasks for this taskable and reschedule the others. No
    // memory barriers needed here as we're only concerned with the tasks
    // queue at this point so release/acquire consistency is adequate.
    while (auto elem = tasks.try_dequeue()) {
        if (elem->isInternalExecutorTask()) {
            // No task associated, must be a resetTaskPtr task, we need to
            // run this now to get it out of the queue so that we can
            // shutdown the bucket even if we are currently running long
            // running tasks.
            tasksToRun.push_back(std::move(*elem));
            continue;
        }

        // We want to cancel anything that is associated with this taskable
        // and already dead. If a task is not dead then it must be run
        // before shutdown.
        if (&elem->task->getTaskable() == &taskable && elem->task->isdead()) {
            tasksToCancel.push_back(elem->task);
            continue;
        }

        tasksToPushBack.push_back(std::move(*elem));
    }

    for (auto& taskToRun : tasksToRun) {
        taskToRun.func();
    }

    for (auto& taskToPushBack : tasksToPushBack) {
        add(taskToPushBack.task, std::move(taskToPushBack.func));
    }

    return tasksToCancel;
}
