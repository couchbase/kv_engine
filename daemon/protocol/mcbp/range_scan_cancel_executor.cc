/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2022-Present Couchbase, Inc.
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
#include <mcbp/protocol/request.h>
#include <memcached/range_scan_id.h>

void range_scan_cancel_executor(Cookie& cookie) {
    auto status = cookie.swapAiostat(cb::engine_errc::success);

    if (status == cb::engine_errc::success) {
        const auto& req = cookie.getRequest();
        cb::rangescan::Id id = req.getCommandSpecifics<cb::rangescan::Id>();
        status = cancelRangeScan(cookie, req.getVBucket(), id);
    }

    handle_executor_status(cookie, status);
}
