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

#include <daemon/memcached.h>
#include <mcbp/protocol/opcode.h>
#include <mcbp/protocol/request.h>

/**
 * SaslRefreshCommandContext is responsible for handling the
 * isasl refresh command. Due to the fact that the task involves file IO
 * it'll offload the task to another thread to do the actual work which
 * notifies the command cookie when it's done.
 */
class SaslRefreshCommandContext : public SteppableCommandContext {
public:
    enum class State {
            Refresh,
            Done
    };

    explicit SaslRefreshCommandContext(Cookie& cookie)
        : SteppableCommandContext(cookie) {
    }

protected:
    cb::engine_errc step() override {
        auto ret = cb::engine_errc::success;
        do {
            switch (state) {
            case State::Refresh:
                ret = refresh();
                break;
            case State::Done:

                done();
                return cb::engine_errc::success;
            }
        } while (ret == cb::engine_errc::success);

        return ret;
    }

    cb::engine_errc refresh();
    void done();

private:
    State state = State::Refresh;
};
