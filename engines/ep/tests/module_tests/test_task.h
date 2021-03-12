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

class TestTask : public GlobalTask {
public:
    TestTask(Taskable& t, TaskId id, int o = 0)
        : GlobalTask(t, id, 0.0, false),
          order(o),
          description(std::string("TestTask ") +
                      GlobalTask::getTaskName(getTaskId())) {
    }

    // returning true will also drive the ExecutorPool::reschedule path.
    bool run() override {
        return true;
    }

    std::string getDescription() const override {
        return description;
    }

    std::chrono::microseconds maxExpectedDuration() const override {
        return std::chrono::seconds(0);
    }

    int order;
    const std::string description;
};
