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

#include <gsl/gsl-lite.hpp>

void CancellableCPUExecutor::addWithTask(GlobalTask& task, folly::Func func) {
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
        // woken if not busy to run it). The scheduler thread does not
        // remove tasks from the queue.
        std::atomic_thread_fence(std::memory_order_seq_cst);
        auto task = tasks.try_dequeue();

        Expects(task);
        task->func();
    });
}
