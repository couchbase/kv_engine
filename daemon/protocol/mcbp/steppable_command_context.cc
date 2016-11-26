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

void SteppableCommandContext::drive() {
    ENGINE_ERROR_CODE ret = connection.getAiostat();
    connection.setAiostat(ENGINE_SUCCESS);
    connection.setEwouldblock(false);

    if (ret == ENGINE_SUCCESS) {
        ret = step();
    }

    switch (ret) {
    case ENGINE_SUCCESS:
        break;
    case ENGINE_EWOULDBLOCK:
        connection.setAiostat(ENGINE_EWOULDBLOCK);
        connection.setEwouldblock(true);
        return;
    case ENGINE_DISCONNECT:
        connection.setState(conn_closing);
        return;
    default:
        mcbp_write_packet(&connection, engine_error_2_mcbp_protocol_error(ret));
        return;
    }
}
