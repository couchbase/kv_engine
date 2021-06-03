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

#include <executor/globaltask.h>
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

    bool run() override {
        return func(*this);
    }

    std::string getDescription() const override {
        return "Lambda Task";
    }

    std::chrono::microseconds maxExpectedDuration() const override {
        // Only used for testing; return an arbitrary large value.
        return std::chrono::seconds(60);
    }

protected:
    std::function<bool(LambdaTask&)> func;
};
