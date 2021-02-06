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
