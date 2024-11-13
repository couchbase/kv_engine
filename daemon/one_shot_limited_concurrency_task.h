/*
 *     Copyright 2021-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#pragma once

#include "nobucket_taskable.h"
#include <executor/limited_concurrency_task.h>
#include <logger/logger.h>
#include <functional>

class OneShotLimitedConcurrencyTask : public LimitedConcurrencyTask {
public:
    /**
     * Create a new instance of the OneShotLimitedConcurrencyTask
     *
     * @param id The task identifier for the task
     * @param name The name for the task
     * @param function The function to run on the executor
     * @param semaphore The semaphore to use concurrency limiting
     */
    OneShotLimitedConcurrencyTask(TaskId id,
                                  std::string name,
                                  std::function<void()> function,
                                  cb::AwaitableSemaphore& semaphore,
                                  std::chrono::microseconds expectedRuntime =
                                          std::chrono::milliseconds(100))
        : LimitedConcurrencyTask(
                  NoBucketTaskable::instance(), id, semaphore, true),
          name(std::move(name)),
          function(std::move(function)),
          expectedRuntime(std::move(expectedRuntime)) {
    }

    std::string getDescription() const override {
        return name;
    }
    std::chrono::microseconds maxExpectedDuration() const override {
        return expectedRuntime;
    }

protected:
    bool runInner() override {
        auto guard = acquireOrWait();
        if (!guard) {
            // could not acquire a token, queued for notification.
            // already snooze()-ed forever, just return true to
            // reschedule.
            return true;
        }

        // Do concurrency-limited work
        try {
            function();
        } catch (const std::exception& e) {
            LOG_CRITICAL_CTX(
                    "OneShotLimitedConcurrencyTask::run():"
                    " received exception",
                    {"task", name},
                    {"error", e.what()});
        }
        return false;
    }

    const std::string name;
    const std::function<void()> function;
    const std::chrono::microseconds expectedRuntime;
};