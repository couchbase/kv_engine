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
#include <memcached/protocol_binary.h>

void dcp_snapshot_marker_executor(Cookie& cookie) {
    auto ret = cookie.swapAiostat(ENGINE_SUCCESS);

    auto& connection = cookie.getConnection();
    if (ret == ENGINE_SUCCESS) {
        auto& request = cookie.getRequest();
        using cb::mcbp::request::DcpSnapshotMarkerV1Payload;
        using cb::mcbp::request::DcpSnapshotMarkerV2Payload;
        auto extra = request.getExtdata();

        const auto* payload =
                reinterpret_cast<const DcpSnapshotMarkerV1Payload*>(
                        extra.data());
        boost::optional<uint64_t> hcs;
        if (extra.size() == sizeof(DcpSnapshotMarkerV2Payload)) {
            const auto* v2Payload =
                    reinterpret_cast<const DcpSnapshotMarkerV2Payload*>(
                            extra.data());

            // HCS should never be sent as 0 or a pre-condition will throw in
            // the replicas flusher
            if (v2Payload->getHighCompletedSeqno() == 0) {
                // Not success so just disconnect
                connection.shutdown();
                return;
            }
            hcs = v2Payload->getHighCompletedSeqno();
        }

        ret = dcpSnapshotMarker(cookie,
                                request.getOpaque(),
                                request.getVBucket(),
                                payload->getStartSeqno(),
                                payload->getEndSeqno(),
                                payload->getFlags(),
                                hcs);
    }

    ret = connection.remapErrorCode(ret);
    switch (ret) {
    case ENGINE_SUCCESS:
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

