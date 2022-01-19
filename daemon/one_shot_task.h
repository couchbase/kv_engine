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
#include <logger/logger.h>
#include <functional>

/**
 * The OneShotTask accepts is a "generic" task which runs the provided
 * function _once_ to avoid having to duplicate all of the boilerplate
 * code for a simple task.
 */
class OneShotTask : public GlobalTask {
public:
    /**
     * Create a new instance of the OneShotTask
     *
     * @param id The task identifier for the task
     * @param name The name for the task
     * @param function The function to run on the executor
     */
    OneShotTask(TaskId id,
                std::string name,
                std::function<void()> function,
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
    bool run() override {
        try {
            function();
        } catch (const std::exception& e) {
            LOG_CRITICAL(R"(OneShotTask::run("{}") received exception: {})",
                         name,
                         e.what());
        }
        return false;
    }

public:
    const std::string name;
    const std::function<void()> function;
    const std::chrono::microseconds expectedRuntime;
};
