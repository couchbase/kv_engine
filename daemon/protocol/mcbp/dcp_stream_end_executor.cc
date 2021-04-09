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

#include "engine_wrapper.h"
#include "executors.h"

#include <daemon/cookie.h>
#include <memcached/protocol_binary.h>

void dcp_stream_end_executor(Cookie& cookie) {
    auto ret = cookie.swapAiostat(cb::engine_errc::success);

    if (ret == cb::engine_errc::success) {
        auto& request = cookie.getRequest();
        auto extras = request.getExtdata();
        using cb::mcbp::request::DcpStreamEndPayload;
        const auto* payload =
                reinterpret_cast<const DcpStreamEndPayload*>(extras.data());
        ret = dcpStreamEnd(cookie,
                           request.getOpaque(),
                           request.getVBucket(),
                           payload->getStatus());
    }

    if (ret != cb::engine_errc::success) {
        handle_executor_status(cookie, ret);
    }
}

