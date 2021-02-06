/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2016 Couchbase, Inc.
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

#include "steppable_command_context.h"

#include <daemon/cookie.h>
class Task;

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

    void setTask(std::shared_ptr<Task> t) {
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

    std::shared_ptr<Task> task;
};
