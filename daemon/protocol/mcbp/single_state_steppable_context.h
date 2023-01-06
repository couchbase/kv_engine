/*
 *     Copyright 2023-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#pragma once

#include "steppable_command_context.h"
#include <functional>

/**
 * SingleStateCommandContext is used to implement commands which just
 * use a single state method which may or may not block.
 *
 * The handler function provided to the class contains the implementation
 * of the command, and SingleStateCommandContext deals with all return
 * codes (including would_block and call the handler at a later time
 * once the cookie has been notified).
 *
 * The handler should return "success" when the execution of the command
 * is complete; and at that time the SingleStateCommandContext will send
 * a "success" message back to the client and use whatever the cookie
 * stored in its error context as the payload to the message.
 */
class SingleStateCommandContext : public SteppableCommandContext {
public:
    explicit SingleStateCommandContext(
            Cookie& cookie, std::function<cb::engine_errc(Cookie&)> handler);

protected:
    cb::engine_errc step() override;
    const std::function<cb::engine_errc(Cookie&)> handler;
    enum class State { Wait, Done };
    State state = State::Wait;
};
