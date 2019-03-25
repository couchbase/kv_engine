/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2017 Couchbase, Inc.
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
#include <daemon/memcached.h>
#include <daemon/task.h>

/**
 * CreateBucketCommandContext is responsible for handling the
 * create and delete bucket command. Due to the fact that they are slow
 * operations it'll offload the work to another thread to do the actual
 * work before it'll notify the connection object which sends the result
 * of the operation back to the client
 */
class CreateRemoveBucketCommandContext : public SteppableCommandContext {
public:
    // The state machine start off in the Initial state where it checks
    // if the command is create or delete and moves into the appropriate
    // state and start the task before entering the done state and wait
    // there for the notification from the task
    enum class State { Initial, Create, Remove, Done };

    explicit CreateRemoveBucketCommandContext(Cookie& cookie)
        : SteppableCommandContext(cookie),
          request(cookie.getRequest()),
          state(State::Initial) {
    }

protected:
    ENGINE_ERROR_CODE step() override;

    ENGINE_ERROR_CODE initial();
    ENGINE_ERROR_CODE remove();
    ENGINE_ERROR_CODE create();

private:
    const cb::mcbp::Request& request;
    State state;
    std::shared_ptr<Task> task;
};
