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

#include <daemon/cookie.h>
#include <memcached/dockey.h>
#include <platform/compress.h>

/**
 * The UnlockCommandContext is a state machine used by the memcached
 * core to implement the Unlock Key operation
 */
class UnlockCommandContext : public SteppableCommandContext {
public:
    // The internal states. Look at the function headers below to
    // for the functions with the same name to figure out what each
    // state does
    enum class State : uint8_t {
        Unlock,
        Done
    };

    explicit UnlockCommandContext(Cookie& cookie)
        : SteppableCommandContext(cookie),
          vbucket(cookie.getRequest().getVBucket()),
          cas(cookie.getRequest().getCas()),
          state(State::Unlock) {
    }

protected:
    /**
     * Keep running the state machine.
     *
     * @return A standard engine error code (if SUCCESS we've changed the
     *         the connections state to one of the appropriate states (send
     *         data, or start processing the next command)
     */
    cb::engine_errc step() override;

    /**
     * Unlock the document (this is the state the statemachine would
     * be "stuck" in if the underlying engine returns EWOULDBLOCK
     *
     * @return The return value of the engine interface unlock method
     */
    cb::engine_errc unlock();

private:
    const Vbid vbucket;
    const uint64_t cas;

    State state;
};
