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
