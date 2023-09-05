/*
 *     Copyright 2023-Present Couchbase, Inc.
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
#include <functional>
#include <optional>

/**
 * YieldingLimitedConcurrencyTask accepts a "generic" task which runs the
 * provided function until completed to avoid having to duplicate all of the
 * boilerplate code for a simple task.
 *
 * A semaphore is used to limit the number of tasks run concurrently.
 * The function may return a sleep time after which it wishes to run again
 * (0s may be used to yield the thread and run again asap), or a
 * nullopt to indicate it is complete.
 */
class YieldingLimitedConcurrencyTask : public LimitedConcurrencyTask {
public:
    using YieldingFunc =
            std::function<std::optional<std::chrono::duration<double>>()>;

    /**
     * Create a new instance of the YieldingLimitedConcurrencyTask
     *
     * @param id The task identifier for the task
     * @param name The name for the task
     * @param function The function to run on the executor
     * @param semaphore The semaphore to use for concurrency limiting
     * @param expectedRuntime Maximum duration `function` is expected to run
     */
    YieldingLimitedConcurrencyTask(TaskId id,
                                   std::string name,
                                   YieldingFunc function,
                                   cb::AwaitableSemaphore& semaphore,
                                   std::chrono::microseconds expectedRuntime =
                                           std::chrono::milliseconds(100));

    std::string getDescription() const override {
        return name;
    }

    std::chrono::microseconds maxExpectedDuration() const override {
        return expectedRuntime;
    }

protected:
    bool runInner() override;

    const std::string name;
    const YieldingFunc function;
    const std::chrono::microseconds expectedRuntime;
};
