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

#include "dcp_add_failover_log.h"
#include "engine_wrapper.h"

#include <daemon/cookie.h>
#include <logger/logger.h>
#include <memcached/protocol_binary.h>

void dcp_stream_req_executor(Cookie& cookie) {
    uint64_t rollback_seqno = 0;

    auto ret = cookie.swapAiostat(ENGINE_SUCCESS);

    auto& connection = cookie.getConnection();
    if (ret == ENGINE_ROLLBACK) {
        LOG_WARNING(
                "{}: dcp_stream_req_executor: Unexpected AIO stat"
                " result ROLLBACK. Shutting down DCP connection",
                connection.getId());
        connection.shutdown();
        return;
    }

    if (ret == ENGINE_SUCCESS) {
        const auto& request = cookie.getRequest();
        auto extras = request.getExtdata();
        using cb::mcbp::request::DcpStreamReqPayload;
        const auto* payload =
                reinterpret_cast<const DcpStreamReqPayload*>(extras.data());

        uint32_t flags = payload->getFlags();
        uint64_t start_seqno = payload->getStartSeqno();
        uint64_t end_seqno = payload->getEndSeqno();
        uint64_t vbucket_uuid = payload->getVbucketUuid();
        uint64_t snap_start_seqno = payload->getSnapStartSeqno();
        uint64_t snap_end_seqno = payload->getSnapEndSeqno();

        // Collection enabled DCP allows a value containing stream config data.
        boost::optional<std::string_view> collections;
        // Initialise the optional only if collections is enabled and even
        // if valuelen is 0
        if (cookie.getConnection().isCollectionsSupported()) {
            auto value = request.getValue();
            collections = std::string_view{
                    reinterpret_cast<const char*>(value.data()), value.size()};
        }

        ret = dcpStreamReq(cookie,
                           flags,
                           request.getOpaque(),
                           request.getVBucket(),
                           start_seqno,
                           end_seqno,
                           vbucket_uuid,
                           snap_start_seqno,
                           snap_end_seqno,
                           &rollback_seqno,
                           add_failover_log,
                           collections);
    }

    ret = connection.remapErrorCode(ret);
    switch (ret) {
    case ENGINE_SUCCESS:
        break;

    case ENGINE_ROLLBACK:
        rollback_seqno = htonll(rollback_seqno);
        cookie.sendResponse(cb::mcbp::Status::Rollback,
                            {},
                            {},
                            {reinterpret_cast<const char*>(&rollback_seqno),
                             sizeof(rollback_seqno)},
                            cb::mcbp::Datatype::Raw,
                            0);
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

