/*
 *     Copyright 2011-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#pragma once

#include "steppable_command_context.h"

#include <mcbp/protocol/status.h>

/**
 * FileReloadCommandContext is a steppable command context
 * used for tasks reloading files
 */
class FileReloadCommandContext : public SteppableCommandContext {
public:
    enum class State { Reload, Done };

    explicit FileReloadCommandContext(Cookie& cookie)
        : SteppableCommandContext(cookie) {
    }

protected:
    cb::engine_errc step() override {
        auto ret = cb::engine_errc::success;
        do {
            switch (state) {
            case State::Reload:
                state = State::Done;
                ret = reload();
                break;
            case State::Done:
                done();
                return cb::engine_errc::success;
            }
        } while (ret == cb::engine_errc::success);

        return ret;
    }

    virtual cb::engine_errc reload() = 0;

    /// The status code to send back to the client
    cb::mcbp::Status command_status_code = cb::mcbp::Status::Success;

    virtual void done() {
        cookie.sendResponse(command_status_code);
    }

private:
    State state = State::Reload;
};
