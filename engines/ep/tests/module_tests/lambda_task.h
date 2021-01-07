/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2017 Couchbase, Inc
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

#pragma once

#include "globaltask.h"

#include <functional>
#include <utility>

class LambdaTask : public GlobalTask {
public:
    LambdaTask(Taskable& t,
               TaskId taskId,
               double sleeptime,
               bool completeBeforeShutdown,
               std::function<bool(LambdaTask&)> f)
        : GlobalTask(t, taskId, sleeptime, completeBeforeShutdown),
          func(std::move(f)) {
    }

    LambdaTask(EventuallyPersistentEngine* engine,
               TaskId taskId,
               double sleeptime,
               bool completeBeforeShutdown,
               std::function<bool(LambdaTask&)> f)
        : GlobalTask(engine, taskId, sleeptime, completeBeforeShutdown),
          func(std::move(f)) {
    }

    bool run() noexcept override {
        return func(*this);
    }

    std::string getDescription() override {
        return "Lambda Task";
    }

    std::chrono::microseconds maxExpectedDuration() override {
        // Only used for testing; return an arbitrary large value.
        return std::chrono::seconds(60);
    }

protected:
    std::function<bool(LambdaTask&)> func;
};
