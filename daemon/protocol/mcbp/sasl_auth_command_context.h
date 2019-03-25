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
#include <daemon/task.h>

/**
 * SaslAuthCommandContext is responsible for handling the
 * SASL auth and SASL continue commands. Due to the fact that they may
 * generate various iterative hashes they may consume a fair amount of
 * CPU time so it'll offload the task to another thread to do the
 * actual work which notifies the command cookie when it's done.
 */
class SaslAuthCommandContext : public SteppableCommandContext {
public:

    // The authentication phase starts off in the Initial state where
    // we create a task and pass off to our executor before we wait
    // for it to complete. Once it completes we'll parse the auth
    // result in ParseAuthTaskResult and flip into each of the
    // different states in order to handle the result from cbsasl
    // and send the appropriate response back to the client
    enum class State {
        Initial,
        ParseAuthTaskResult,
        AuthOk,
        AuthContinue,
        AuthBadParameters,
        AuthFailure,
        Done };

    explicit SaslAuthCommandContext(Cookie& cookie)
        : SteppableCommandContext(cookie),
          request(cookie.getRequest()),
          state(State::Initial) {
    }

protected:
    ENGINE_ERROR_CODE step() override;

    ENGINE_ERROR_CODE initial();
    ENGINE_ERROR_CODE parseAuthTaskResult();
    ENGINE_ERROR_CODE authOk();
    ENGINE_ERROR_CODE authContinue();
    ENGINE_ERROR_CODE authBadParameters();
    ENGINE_ERROR_CODE authFailure();

private:
    const cb::mcbp::Request& request;
    State state;
    std::shared_ptr<Task> task;
};
