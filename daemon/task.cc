/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2015-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "task.h"
#include "executor.h"

void Task::makeRunnable() {
    if (executor == nullptr) {
        throw std::logic_error("task need to be scheduled");
    }
    executor->makeRunnable(this);
}

void Task::makeRunnable(std::chrono::steady_clock::time_point time) {
    if (executor == nullptr) {
        throw std::logic_error("task need to be scheduled");
    }
    scheduledTime = time;
    executor->makeRunnable(*this, time);
}

Task::Status PeriodicTask::execute() {
    auto next_time = next();
    Status status = periodicExecute();
    if (status == Status::Continue) {
        makeRunnable(next_time);
    }
    return status;
}

std::chrono::steady_clock::time_point PeriodicTask::next() {
    if (scheduledTime == std::chrono::steady_clock::time_point()) {
        return std::chrono::steady_clock::now() + period;
    }
    // This reduces jitter / clock drift
    return scheduledTime + period;
}
