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

#include "executors.h"

#include "engine_wrapper.h"

#include <daemon/cookie.h>
#include <mcbp/dcp_snapshot_marker_codec.h>
#include <memcached/protocol_binary.h>

void dcp_snapshot_marker_executor(Cookie& cookie) {
    auto ret = cookie.swapAiostat(ENGINE_SUCCESS);

    auto& connection = cookie.getConnection();
    if (ret == ENGINE_SUCCESS) {
        auto& request = cookie.getRequest(Cookie::PacketContent::Full);
        using cb::mcbp::request::DcpSnapshotMarkerV1Payload;
        using cb::mcbp::request::DcpSnapshotMarkerV2xPayload;
        auto extra = request.getExtdata();

        cb::mcbp::DcpSnapshotMarker snapshot;
        if (extra.size() == sizeof(DcpSnapshotMarkerV2xPayload)) {
            // Validators will have checked that version is 0, the only version
            // this code can receive.
            snapshot = cb::mcbp::decodeDcpSnapshotMarkerV2xValue(
                    request.getValue());
        } else {
            snapshot = cb::mcbp::decodeDcpSnapshotMarkerV1Extra(extra);
        }

        ret = dcpSnapshotMarker(cookie,
                                request.getOpaque(),
                                request.getVBucket(),
                                snapshot.getStartSeqno(),
                                snapshot.getEndSeqno(),
                                snapshot.getFlags(),
                                snapshot.getHighCompletedSeqno(),
                                snapshot.getMaxVisibleSeqno());
    }

    ret = connection.remapErrorCode(ret);
    switch (ret) {
    case ENGINE_SUCCESS:
        connection.setState(StateMachine::State::ship_log);
        break;

    case ENGINE_DISCONNECT:
        connection.setState(StateMachine::State::closing);
        break;

    case ENGINE_EWOULDBLOCK:
        cookie.setEwouldblock(true);
        break;

    default:
        cookie.sendResponse(cb::engine_errc(ret));
    }
}

