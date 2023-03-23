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
#include <chrono>
#include <functional>
#include <optional>

/**
 * YieldingTask accepts is a "generic" task which runs the provided
 * function until completed to avoid having to duplicate all of the boilerplate
 * code for a simple task.
 *
 * The function may return a sleep time after which it wishes to run again
 * (0s may be used to yield the thread and run again asap), or a
 * nullopt to indicate it is complete.
 */
class YieldingTask : public GlobalTask {
public:
    using YieldingFunc =
            std::function<std::optional<std::chrono::duration<double>>()>;
    /**
     * Create a new instance of the YieldingTask
     *
     * @param id The task identifier for the task
     * @param name The name for the task
     * @param function The function to run on the executor
     */
    YieldingTask(TaskId id,
                 std::string name,
                 YieldingFunc function,
                 std::chrono::microseconds expectedRuntime =
                         std::chrono::milliseconds(100))
        : GlobalTask(NoBucketTaskable::instance(), id),
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
    bool run() override;

public:
    const std::string name;
    const YieldingFunc function;
    const std::chrono::microseconds expectedRuntime;
};
