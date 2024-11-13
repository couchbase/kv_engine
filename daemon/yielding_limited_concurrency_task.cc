/*
 *     Copyright 2023-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "yielding_limited_concurrency_task.h"
#include <logger/logger.h>

YieldingLimitedConcurrencyTask::YieldingLimitedConcurrencyTask(
        TaskId id,
        std::string name,
        YieldingFunc function,
        cb::AwaitableSemaphore& semaphore,
        std::chrono::microseconds expectedRuntime)
    : LimitedConcurrencyTask(NoBucketTaskable::instance(), id, semaphore, true),
      name(std::move(name)),
      function(std::move(function)),
      expectedRuntime(std::move(expectedRuntime)) {
}

bool YieldingLimitedConcurrencyTask::runInner() {
    auto guard = acquireOrWait();
    if (!guard) {
        // Could not acquire a token, queued for notification.
        // Already snooze()-ed forever, just return true to
        // reschedule.
        return true;
    }

    // Do concurrency-limited work
    try {
        auto optSleepTime = function();
        if (optSleepTime) {
            snooze(optSleepTime->count());
            // Task _does_ wish to run again
            return true;
        }
    } catch (const std::exception& e) {
        LOG_CRITICAL_CTX(
                "YieldingLimitedConcurrencyTask::runInner(): received "
                "exception",
                {"task", name},
                {"error", e.what()});
    }

    // Not running again
    return false;
}
