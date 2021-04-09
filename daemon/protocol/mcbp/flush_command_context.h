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

#include <daemon/buckets.h>
#include <daemon/connection.h>
#include <daemon/memcached.h>
#include <mcbp/protocol/opcode.h>
#include <mcbp/protocol/request.h>

/**
 * FlushCommandContext is responsible for handling the
 * flush command. It may block and offload the configure
 * event to a different thread and be notified later on.
 */
class FlushCommandContext : public SteppableCommandContext {
public:
    enum class State {
        /// The command context starts off in the Flushing state
        /// which triggers the flush in the underlying engine (which
        /// may offload the flush to a different thread).
        Flushing,
        /// Send the response back to the client
        Done
    };

    explicit FlushCommandContext(Cookie& cookie);

protected:
    cb::engine_errc step() override {
        auto ret = cb::engine_errc::success;
        do {
            switch (state) {
            case State::Flushing:
                ret = flushing();
                break;
            case State::Done:
                done();
                return cb::engine_errc::success;
            }
        } while (ret == cb::engine_errc::success);

        return ret;
    }

    cb::engine_errc flushing();
    void done();

private:
    State state;
};
