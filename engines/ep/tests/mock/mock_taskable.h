/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2020 Couchbase, Inc
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

#include "taskable.h"

/**
 * Mock task owner for testing purposes.
 */
class MockTaskable : public Taskable {
public:
    MockTaskable();

    const std::string& getName() const override;

    task_gid_t getGID() const override;

    bucket_priority_t getWorkloadPriority() const override;

    void setWorkloadPriority(bucket_priority_t prio) override;

    WorkLoadPolicy& getWorkLoadPolicy() override;

    void logQTime(TaskId id,
                  const std::chrono::steady_clock::duration enqTime) override;

    void logRunTime(TaskId id,
                    const std::chrono::steady_clock::duration runTime) override;

protected:
    std::string name;
    WorkLoadPolicy policy;
};
