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

void dcp_get_failover_log_executor(Cookie& cookie) {
    auto ret = cookie.swapAiostat(cb::engine_errc::success);

    if (ret == cb::engine_errc::success) {
        auto& req = cookie.getRequest();
        ret = dcpGetFailoverLog(
                cookie,
                req.getOpaque(),
                req.getVBucket(),
                [c = std::ref(cookie)](
                        const std::vector<vbucket_failover_t>& vec) {
                    return add_failover_log(vec, c);
                });
    }

    if (ret != cb::engine_errc::success) {
        handle_executor_status(cookie, ret);
    }
}
