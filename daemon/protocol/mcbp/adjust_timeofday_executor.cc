/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2017 Couchbase, Inc.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

#include "executors.h"

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
    auto extras = cookie.getRequest().getExtdata();
    using cb::mcbp::request::AdjustTimePayload;
    auto* payload = reinterpret_cast<const AdjustTimePayload*>(extras.data());

    switch (payload->getTimeType()) {
    case AdjustTimePayload::TimeType::TimeOfDay:
        cb_set_timeofday_offset(gsl::narrow_cast<int>(payload->getOffset()));
        mc_time_clock_tick();
        cookie.sendResponse(cb::mcbp::Status::Success);
        return;
    case AdjustTimePayload::TimeType::Uptime:
        cb_set_uptime_offset(payload->getOffset());
        mc_time_clock_tick();
        cookie.sendResponse(cb::mcbp::Status::Success);
        return;
    }

    cookie.sendResponse(cb::mcbp::Status::Einval);
}
