/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2016 Couchbase, Inc.
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

#include <memcached/types.h>
#include "command_context.h"

// Forward declaration
class McbpConnection;

/**
 * The steppable command context is an iterface to a command context
 * which provides a 'step' method which allows the command context
 * to implement itself as a state machine.
 *
 * It
 */
class SteppableCommandContext : public CommandContext {
public:
    SteppableCommandContext(McbpConnection& c) : connection(c) {
    }

    virtual ~SteppableCommandContext() {
    }

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
    virtual ENGINE_ERROR_CODE step() = 0;

    /**
     * The connection this command context is bound to (it is used to send
     * response / set ewouldblock etc
     */
    McbpConnection& connection;
};
