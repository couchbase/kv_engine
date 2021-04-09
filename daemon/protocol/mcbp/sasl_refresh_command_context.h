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
