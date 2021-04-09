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

#include <string>

/**
 * Mock Task class. Doesn't actually run() or snooze() - they both do nothing.
 */
class MockGlobalTask : public GlobalTask {
public:
    MockGlobalTask(Taskable& t, TaskId id) : GlobalTask(t, id) {
    }

    bool run() override {
        return false;
    }
    std::string getDescription() const override {
        return "MockGlobalTask";
    }

    std::chrono::microseconds maxExpectedDuration() const override {
        // Shouldn't matter what this returns
        return std::chrono::seconds(0);
    }

    void snooze(const double secs) override {
    }
};
