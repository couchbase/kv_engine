/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2022-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#pragma once

#include <executor/globaltask.h>

#include <climits>

/**
 * Abstract class for tasks that needs to be reliably notified to run by the
 * user.
 * A call to wakeup() schedules another run only if the task has not entered the
 * inner/user-defined processing function. The call is a nop otherwise.
 * The implementation ensures that wake-ups aren't lost, but it doesn't prevent
 * 1 extra run from being unnecessarily scheduled, eg:
 *   1. task is in the middle of the processing
 *   2. user detects a state that requires a task run -> run is scheduled
 *   3. the current processing at (1) completes and makes the newly scheduled
 *      run (2) unnecessary
 *   4. run (2) executes
 */
class NotifiableTask : public GlobalTask {
public:
    NotifiableTask(EventuallyPersistentEngine* e,
                   TaskId id,
                   double sleeptime,
                   bool completeBeforeShutdown = false)
        : GlobalTask(e, id, sleeptime, completeBeforeShutdown){};

    bool run() override;

    void wakeup();

protected:
    /**
     * User defined processing function, executed at run().
     *
     * @return true if the task needs to be re-scheduled, false otherwise
     */
    virtual bool runInner() = 0;

    virtual size_t getSleepTime() const {
        return INT_MAX;
    };

    std::atomic<bool> pendingRun{false};
};