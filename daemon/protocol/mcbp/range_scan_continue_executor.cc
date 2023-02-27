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
#include <memcached/range_scan_status.h>

void range_scan_continue_executor(Cookie& cookie) {
    const auto& req = cookie.getRequest();
    const auto& payload = req.getCommandSpecifics<
            cb::mcbp::request::RangeScanContinuePayload>();

    auto status = continueRangeScan(
            cookie,
            cb::rangescan::ContinueParameters{
                    req.getVBucket(),
                    payload.getId(),
                    payload.getItemLimit(),
                    std::chrono::milliseconds(payload.getTimeLimit()),
                    payload.getByteLimit(),
                    cookie.swapAiostat(cb::engine_errc::success)});

    switch (cb::rangescan::getContinueHandlingStatus(status)) {
    case cb::rangescan::HandlingStatus::EngineSends:
        // The engine has transmitted this status (along with any scanned data)
        return;
    case cb::rangescan::HandlingStatus::ExecutorSends:
        // This executor must handle this status
        handle_executor_status(cookie, status);
        break;
    }
}
