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
#include "steppable_command_context.h"
#include <daemon/mcbp.h>
#include <daemon/memcached.h>
#include <daemon/stats.h>

SteppableCommandContext::SteppableCommandContext(Cookie& cookie_)
    : cookie(cookie_), connection(cookie.getConnection()) {
    cookie.logCommand();
}

void SteppableCommandContext::drive() {
    ENGINE_ERROR_CODE ret = cookie.swapAiostat(ENGINE_SUCCESS);

    if (ret == ENGINE_SUCCESS) {
        try {
            ret = step();
        } catch (const cb::engine_error& error) {
            if (error.code() != cb::engine_errc::would_block) {
                LOG_WARNING(&connection,
                            "%u: SteppableCommandContext::drive() %s: %s",
                            connection.getId(),
                            connection.getDescription().c_str(), error.what());
            }
            ret = ENGINE_ERROR_CODE(error.code().value());
        }

        if (ret == ENGINE_LOCKED || ret == ENGINE_LOCKED_TMPFAIL) {
            STATS_INCR(&connection, lock_errors);
        }
    }

    cookie.logResponse(ret);
    ret = connection.remapErrorCode(ret);
    switch (ret) {
    case ENGINE_SUCCESS:
        break;
    case ENGINE_EWOULDBLOCK:
        cookie.setEwouldblock(true);
        return;
    case ENGINE_DISCONNECT:
        connection.setState(McbpStateMachine::State::closing);
        return;
    default:
        cookie.sendResponse(cb::engine_errc(ret));
        return;
    }
}

void SteppableCommandContext::setDatatypeJSONFromValue(
        const cb::const_byte_buffer& value,
        protocol_binary_datatype_t& datatype) {
    // Determine if document is JSON or not. We do not trust what the client
    // sent - instead we check for ourselves.
    if (connection.getThread()->validator->validate(value.buf, value.len)) {
        datatype |= PROTOCOL_BINARY_DATATYPE_JSON;
    } else {
        datatype &= ~PROTOCOL_BINARY_DATATYPE_JSON;
    }
}
