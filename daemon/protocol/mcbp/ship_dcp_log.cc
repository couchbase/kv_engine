/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2018 Couchbase, Inc.
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

#include "ship_dcp_log.h"

#include "daemon/buckets.h"
#include "daemon/connection.h"
#include "daemon/cookie.h"

void ship_dcp_log(Cookie& cookie) {
    auto& c = cookie.getConnection();
    c.addMsgHdr(true);
    cookie.setEwouldblock(false);
    const auto ret =  c.remapErrorCode(c.getBucket().getDcpIface()->step(
        static_cast<const void*>(&c.getCookieObject()), &c));

    switch (ret) {
    case ENGINE_SUCCESS:
        /* The engine got more data it wants to send */
        c.setState(McbpStateMachine::State::send_data);
        c.setWriteAndGo(McbpStateMachine::State::ship_log);
        break;
    case ENGINE_EWOULDBLOCK:
        /* the engine don't have more data to send at this moment */
        cookie.setEwouldblock(true);
        break;
    default:
        LOG_WARNING(
                "{}: ship_dcp_log - step returned {} - closing connection {}",
                c.getId(),
                std::to_string(ret),
                c.getDescription());
        c.setState(McbpStateMachine::State::closing);
    }
}
