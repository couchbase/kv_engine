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

void dcp_control_executor(Cookie& cookie) {
    auto ret = cookie.swapAiostat(cb::engine_errc::success);

    if (ret == cb::engine_errc::success) {
        const auto& req = cookie.getRequest();

        ret = dcpControl(cookie,
                         req.getOpaque(),
                         req.getKeyString(),
                         req.getValueString());
    }

    handle_executor_status(cookie, ret);
}
