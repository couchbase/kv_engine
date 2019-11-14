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

#include "engine_wrapper.h"
#include "utilities.h"
#include <memcached/protocol_binary.h>

void dcp_commit_executor(Cookie& cookie) {
    auto ret = cookie.swapAiostat(ENGINE_SUCCESS);

    auto& connection = cookie.getConnection();
    if (ret == ENGINE_SUCCESS) {
        const auto& req = cookie.getRequest();
        auto extdata = req.getExtdata();
        using cb::mcbp::request::DcpCommitPayload;
        const auto& extras =
                *reinterpret_cast<const DcpCommitPayload*>(extdata.data());
        ret = dcpCommit(cookie,
                        req.getOpaque(),
                        req.getVBucket(),
                        cookie.getConnection().makeDocKey(req.getKey()),
                        extras.getPreparedSeqno(),
                        extras.getCommitSeqno());
    }

    ret = connection.remapErrorCode(ret);
    switch (ret) {
    case ENGINE_SUCCESS:
        connection.setState(StateMachine::State::new_cmd);
        break;

    case ENGINE_DISCONNECT:
        connection.shutdown();
        break;

    case ENGINE_EWOULDBLOCK:
        cookie.setEwouldblock(true);
        break;

    default:
        cookie.sendResponse(cb::engine_errc(ret));
    }
}
