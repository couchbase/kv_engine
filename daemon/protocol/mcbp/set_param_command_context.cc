/*
 *     Copyright 2025-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "set_param_command_context.h"
#include "engine_wrapper.h"
#include <daemon/buckets.h>
#include <daemon/concurrency_semaphores.h>
#include <daemon/connection.h>
#include <logger/logger.h>
#include <memcached/engine.h>

EngineParamCategory SetParamCommandContext::getParamCategory(Cookie& cookie) {
    const auto& req = cookie.getRequest();
    using cb::mcbp::request::SetParamPayload;
    const auto& payload = req.getCommandSpecifics<SetParamPayload>();
    switch (payload.getParamType()) {
    case SetParamPayload::Type::Flush:
        return EngineParamCategory::Flush;
    case SetParamPayload::Type::Checkpoint:
        return EngineParamCategory::Checkpoint;
    case SetParamPayload::Type::Dcp:
        return EngineParamCategory::Dcp;
    case SetParamPayload::Type::Vbucket:
        return EngineParamCategory::Vbucket;
    case SetParamPayload::Type::Replication:
        Expects(false && "mcbp_validator should reject this group");
    case SetParamPayload::Type::Config:
        return EngineParamCategory::Config;
    }
    throw std::invalid_argument(
            "SetParamCommandContext::getParamCategory(): Invalid param "
            "provided: " +
            std::to_string(int(payload.getParamType())));
}

SetParamCommandContext::SetParamCommandContext(Cookie& cookie)
    : SteppableCommandContext(cookie),
      category(getParamCategory(cookie)),
      key(cookie.getRequest().getKeyString()),
      value(cookie.getRequest().getValueString()),
      vbid(cookie.getRequest().getVBucket()) {
}

cb::engine_errc SetParamCommandContext::step() {
    cb::engine_errc ret = cb::engine_errc::success;
    try {
        // Extract throttling configuration before setting at engine level
        if (key == "throttle_reserved") {
            auto& bucket = cookie.getConnection().getBucket();
            ret = bucket.setThrottleLimits(std::stoul(value),
                                           bucket.getThrottleHardLimit());
        } else if (key == "throttle_hard_limit") {
            auto& bucket = cookie.getConnection().getBucket();
            ret = bucket.setThrottleLimits(bucket.getThrottleReservedLimit(),
                                           std::stoul(value));
        } else {
            // For all other parameters, set at engine level
            ret = bucket_set_parameter(cookie, category, key, value, vbid);
        }
    } catch (const std::exception& e) {
        LOG_WARNING_CTX("SetParamCommandContext: ",
                        {"conn_id", cookie.getConnectionId()},
                        {"error", e.what()});
    }
    if (ret == cb::engine_errc::success) {
        cookie.sendResponse(ret);
    }
    return ret;
}
