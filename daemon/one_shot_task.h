/*
 *     Copyright 2025-Present Couchbase, Inc.
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

class OneShotTask : public GlobalTask {
public:
    /**
     * Create a new instance of the OneShotTask
     *
     * @param id The task identifier for the task
     * @param name_ The name for the task
     * @param function_ The function to run on the executor
     */
    OneShotTask(TaskId id,
                std::string name_,
                std::function<void()> function_,
                std::chrono::microseconds expectedRuntime_ =
                        std::chrono::milliseconds(100))
        : GlobalTask(NoBucketTaskable::instance(), id, 0, true),
          name(std::move(name_)),
          function(std::move(function_)),
          expectedRuntime(std::move(expectedRuntime_)) {
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
            LOG_CRITICAL_CTX("OneShotTask::run(): received exception",
                             {"task", name},
                             {"error", e.what()});
        }
        return false;
    }

    const std::string name;
    const std::function<void()> function;
    const std::chrono::microseconds expectedRuntime;
};