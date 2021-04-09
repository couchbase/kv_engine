/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2017-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
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
