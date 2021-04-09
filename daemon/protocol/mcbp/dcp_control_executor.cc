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

#include "engine_wrapper.h"
#include "utilities.h"

void dcp_control_executor(Cookie& cookie) {
    auto ret = cookie.swapAiostat(cb::engine_errc::success);

    if (ret == cb::engine_errc::success) {
        ret = mcbp::haveDcpPrivilege(cookie);

        if (ret == cb::engine_errc::success) {
            const auto& req = cookie.getRequest();
            const auto key = req.getKey();
            const auto val = req.getValue();

            ret = dcpControl(
                    cookie,
                    req.getOpaque(),
                    {reinterpret_cast<const char*>(key.data()), key.size()},
                    {reinterpret_cast<const char*>(val.data()), val.size()});
        }
    }

    handle_executor_status(cookie, ret);
}
