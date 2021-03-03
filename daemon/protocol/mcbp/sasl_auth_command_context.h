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

#include <cbsasl/error.h>
#include <daemon/cookie.h>
#include <daemon/task.h>

class SaslAuthTask;

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
    // for it to complete. Once it completes we'll handle the
    // result in handleSaslAuthTaskResult, which also send the
    // response back to the client before we enter the "Done"
    // state which just returns and terminates the state
    // machinery.
    enum class State { Initial, HandleSaslAuthTaskResult, Done };

    explicit SaslAuthCommandContext(Cookie& cookie);

protected:
    cb::engine_errc step() override;

    cb::engine_errc initial();
    cb::engine_errc handleSaslAuthTaskResult();
    cb::engine_errc doHandleSaslAuthTaskResult(SaslAuthTask* auth_task);
    cb::engine_errc tryHandleSaslOk();
    cb::engine_errc authContinue();
    cb::engine_errc authBadParameters();
    cb::engine_errc authFailure();

private:
    const cb::mcbp::Request& request;
    const std::string mechanism;
    State state;
    std::shared_ptr<Task> task;
};
