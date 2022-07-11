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
#include "logger/logger.h"

#include <daemon/cookie.h>
#include <daemon/mc_time.h>
#include <memcached/protocol_binary.h>

#include <platform/platform_time.h>

/**
 * The adjust_timeofday_executor implements the ability to mock the internal
 * clock in the memcached server. It is only used during unit testing of
 * the server (over the network), and the validator will return NOT_SUPPORTED
 * unless the environment variable MEMCACHED_UNIT_TESTS is set.
 */
void adjust_timeofday_executor(Cookie& cookie) {
    using cb::mcbp::request::AdjustTimePayload;
    const auto& payload =
            cookie.getRequest().getCommandSpecifics<AdjustTimePayload>();

    switch (payload.getTimeType()) {
    case AdjustTimePayload::TimeType::TimeOfDay:
        LOG_INFO("{} Adjust TimeOfDay offset from {} to {}",
                 cookie.getConnectionId(),
                 cb_get_timeofday_offset(),
                 payload.getOffset());
        cb_set_timeofday_offset(gsl::narrow_cast<int>(payload.getOffset()));
        mc_time_clock_tick();
        cookie.sendResponse(cb::mcbp::Status::Success);
        return;
    case AdjustTimePayload::TimeType::Uptime:
        LOG_INFO("{} Adjust uptime offset from {} to {}",
                 cookie.getConnectionId(),
                 cb_get_uptime_offset(),
                 payload.getOffset());
        cb_set_uptime_offset(payload.getOffset());
        mc_time_clock_tick();
        cookie.sendResponse(cb::mcbp::Status::Success);
        return;
    }

    cookie.sendResponse(cb::mcbp::Status::Einval);
}
