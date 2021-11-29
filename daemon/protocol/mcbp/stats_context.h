/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2016-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#pragma once

#include "steppable_command_context.h"

#include <daemon/cookie.h>
class StatsTask;
class StatGroup;

/**
 * The StatsCommandContext is responsible for implementing all of the
 * various stats commands (including the sub commands).
 */
class StatsCommandContext : public SteppableCommandContext {
public:
    enum class State {
        // Take the raw key from the cookie and turn it into a command key and
        // any arguments
        ParseCommandKey,
        // Check whether the command requested requires a privileged user, and
        // if so, whether the user has permissions to run it
        CheckPrivilege,
        // Execute the stats command call
        DoStats,
        // If DoStats invokes a background task, go to this state to handle the
        // result of that
        GetTaskResult,
        // Command completed, do any post complete tasks
        CommandComplete,
        // We are done :)
        Done
    };

    explicit StatsCommandContext(Cookie& cookie);

    void setTask(std::shared_ptr<StatsTask> t) {
        task = t;
    }

protected:
    /**
     * All of the internal states return cb::engine_errc::success as even if for
     * some reason the stat command fails, there is still work to be done after
     * the fact. All code paths lead to State::Done which returns
     * command_exit_code which is the actual expected return value.
     */
    cb::engine_errc step() override;

    cb::engine_errc parseCommandKey();

    cb::engine_errc checkPrivilege();

    cb::engine_errc doStats();

    cb::engine_errc getTaskResult();

    cb::engine_errc commandComplete();

private:
    /// The StatGroup for the command (set in parseCommandKey)
    const StatGroup* statgroup = nullptr;

    // The time immediately before the stat handler was invoked
    std::chrono::steady_clock::time_point start;

    /**
     * The key as specified in the input buffer (it may contain a sub command)
     */
    std::string command;
    std::string argument;
    State state;
    /**
     * The final cb::engine_errc returned from actually doing the stats call
     */
    cb::engine_errc command_exit_code;

    std::shared_ptr<StatsTask> task;
};
