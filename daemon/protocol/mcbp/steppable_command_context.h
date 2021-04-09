/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2016-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#pragma once

#include <daemon/cookie.h>
#include <memcached/protocol_binary.h>
#include <memcached/types.h>
#include "command_context.h"

// Forward declaration
class Connection;
class Cookie;

/**
 * The steppable command context is an iterface to a command context
 * which provides a 'step' method which allows the command context
 * to implement itself as a state machine.
 *
 * It
 */
class SteppableCommandContext : public CommandContext {
public:
    explicit SteppableCommandContext(Cookie& cookie_);

    ~SteppableCommandContext() override = default;

    /**
     * Drive the state machine as far as possible and handle the
     * "error code" returned by the command context (EWOULDBLOCK, SUCCESS etc)
     */
    void drive();

protected:
    /**
     * Keep running the state machine.
     *
     * @return A standard engine error code (if SUCCESS we've changed the
     *         the connections state to one of the appropriate states (send
     *         data, or start processing the next command)
     */
    virtual cb::engine_errc step() = 0;

    /**
     * Helper function to set/clear the JSON bit in datatype based on if the
     * given value is JSON or not.
     * @throws std::bad_alloc if insufficient memory was available to parse
     *         the value.
     */
    void setDatatypeJSONFromValue(const cb::const_byte_buffer& value,
                                  protocol_binary_datatype_t& datatype);

    /**
     * The cookie executing this command
     */
    Cookie& cookie;

    /**
     * The connection this command context is bound to (deprecated, and
     * should be removed (it is part of the cookie))
     */
    Connection& connection;
};
