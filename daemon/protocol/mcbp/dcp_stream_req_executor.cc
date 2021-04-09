/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2017-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "executors.h"

#include "dcp_add_failover_log.h"
#include "engine_wrapper.h"

#include <daemon/cookie.h>
#include <logger/logger.h>
#include <memcached/protocol_binary.h>

void dcp_stream_req_executor(Cookie& cookie) {
    uint64_t rollback_seqno = 0;

    auto ret = cookie.swapAiostat(cb::engine_errc::success);

    auto& connection = cookie.getConnection();
    if (ret == cb::engine_errc::rollback) {
        LOG_WARNING(
                "{}: dcp_stream_req_executor: Unexpected AIO stat"
                " result ROLLBACK. Shutting down DCP connection",
                connection.getId());
        connection.shutdown();
        return;
    }

    if (ret == cb::engine_errc::success) {
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
        std::optional<std::string_view> collections;
        // Initialise the optional only if collections is enabled and even
        // if valuelen is 0
        if (cookie.getConnection().isCollectionsSupported()) {
            auto value = request.getValue();
            collections = std::string_view{
                    reinterpret_cast<const char*>(value.data()), value.size()};
        }

        ret = dcpStreamReq(
                cookie,
                flags,
                request.getOpaque(),
                request.getVBucket(),
                start_seqno,
                end_seqno,
                vbucket_uuid,
                snap_start_seqno,
                snap_end_seqno,
                &rollback_seqno,
                [c = std::ref(cookie)](
                        const std::vector<vbucket_failover_t>& vec) {
                    return add_failover_log(vec, c);
                },
                collections);
    }

    if (ret == cb::engine_errc::rollback) {
        rollback_seqno = htonll(rollback_seqno);
        cookie.sendResponse(cb::mcbp::Status::Rollback,
                            {},
                            {},
                            {reinterpret_cast<const char*>(&rollback_seqno),
                             sizeof(rollback_seqno)},
                            cb::mcbp::Datatype::Raw,
                            0);
    } else if (ret != cb::engine_errc::success) {
        handle_executor_status(cookie, ret);
    }
}

