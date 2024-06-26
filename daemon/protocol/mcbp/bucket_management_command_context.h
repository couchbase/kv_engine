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

#include <daemon/cookie.h>
#include <daemon/memcached.h>

/**
 * BucketManagementCommandContext is responsible for handling the
 * create and delete bucket command. Due to the fact that they are slow
 * operations it'll offload the work to another thread to do the actual
 * work before it'll notify the connection object which sends the result
 * of the operation back to the client
 */
class BucketManagementCommandContext : public SteppableCommandContext {
public:
    // The state machine start off in the Initial state where it checks
    // the command and moves into the appropriate state and start the
    // task before entering the done state and wait there for the notification
    // from the task
    enum class State { Initial, Create, Remove, Pause, Resume, Done };

    explicit BucketManagementCommandContext(Cookie& cookie)
        : SteppableCommandContext(cookie), request(cookie.getRequest()) {
    }

protected:
    cb::engine_errc step() override;

    cb::engine_errc initial();
    cb::engine_errc remove();
    cb::engine_errc create();
    cb::engine_errc pause();
    cb::engine_errc resume();

private:
    const cb::mcbp::Request& request;
    State state = State::Initial;
};
