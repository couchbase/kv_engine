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
