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

#include "../../memcached.h"
#include "steppable_command_context.h"

/**
 * AuditConfigureCommandContext is responsible for handling the
 * audit configure message. It may block and offload the configure
 * event to a different thread and be notified later on.
 */
class AuditConfigureCommandContext : public SteppableCommandContext {
public:
    enum class State {
        /// The command context starts off in the Configuring state
        /// which triggers the configure in the audit subsystem
        /// before going into the done state sending the response
        /// back to the client. The framework takes care of sending
        /// all other error messages
        Configuring,
        /// Send the response back to the client
        Done };

    explicit AuditConfigureCommandContext(Cookie& cookie)
        : SteppableCommandContext(cookie) {
    }

protected:
    cb::engine_errc step() override {
        auto ret = cb::engine_errc::success;
        do {
            switch (state) {
            case State::Configuring:
                ret = configuring();
                break;
            case State::Done:
                done();
                return cb::engine_errc::success;
            }
        } while (ret == cb::engine_errc::success);

        return ret;
    }

    cb::engine_errc configuring();
    void done();

private:
    State state = State::Configuring;
};
