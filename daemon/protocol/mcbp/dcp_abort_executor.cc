/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2018-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#include "engine_wrapper.h"
#include "executors.h"
#include "utilities.h"
#include <memcached/protocol_binary.h>

void dcp_abort_executor(Cookie& cookie) {
    auto ret = cookie.swapAiostat(cb::engine_errc::success);
    if (ret == cb::engine_errc::success) {
        const auto& req = cookie.getRequest();
        using cb::mcbp::request::DcpAbortPayload;
        const auto& extras = req.getCommandSpecifics<DcpAbortPayload>();
        ret = dcpAbort(cookie,
                       req.getOpaque(),
                       req.getVBucket(),
                       cookie.getConnection().makeDocKey(req.getKey()),
                       extras.getPreparedSeqno(),
                       extras.getAbortSeqno());
    }

    if (ret != cb::engine_errc::success) {
        handle_executor_status(cookie, ret);
    }
}
