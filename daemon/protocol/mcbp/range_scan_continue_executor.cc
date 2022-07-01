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
#include <memcached/protocol_binary.h>
#include <memcached/range_scan.h>
#include <memcached/range_scan_id.h>
#include <memcached/range_scan_optional_configuration.h>

void range_scan_continue_executor(Cookie& cookie) {
    auto status = cookie.swapAiostat(cb::engine_errc::success);

    switch (status) {
    case cb::engine_errc::success: {
        const auto& req = cookie.getRequest();
        const auto& payload = req.getCommandSpecifics<
                cb::mcbp::request::RangeScanContinuePayload>();

        status = continueRangeScan(
                cookie,
                req.getVBucket(),
                payload.getId(),
                payload.getItemLimit(),
                std::chrono::milliseconds(payload.getTimeLimit()),
                payload.getByteLimit());
        break;
    }
    case cb::engine_errc::range_scan_more:
    case cb::engine_errc::range_scan_complete:
        // The final RangeScan response (along with the status code) has
        // already been sent by RangeScanDataHandler::handleStatus; nothing
        // more to do on front-end here.
        return;
    default:
        // Some other non-successful status code - send to client.
        break;
    }
    handle_executor_status(cookie, status);
}
