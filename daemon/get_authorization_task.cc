/*
 *     Copyright 2020-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "get_authorization_task.h"
#include "connection.h"
#include "cookie.h"
#include "external_auth_manager_thread.h"
#include "log_macros.h"
#include "memcached.h"
#include <platform/json_log_conversions.h>
#include <platform/timeutils.h>

void GetAuthorizationTask::logIfSlowResponse() const {
    auto duration = std::chrono::steady_clock::now() - getStartTime();
    if (duration > externalAuthManager->getExternalAuthSlowDuration()) {
        LOG_WARNING_CTX("Slow external user authorization",
                        {"duration", duration},
                        {"username", cb::tagUserData(getUsername())});
    }
}

void GetAuthorizationTask::externalResponse(cb::mcbp::Status statusCode,
                                            std::string_view payload) {
    logIfSlowResponse();
    if (statusCode == cb::mcbp::Status::Success) {
        status = cb::sasl::Error::OK;
    } else {
        try {
            if (statusCode == cb::mcbp::Status::AuthError) {
                status = cb::sasl::Error::NO_RBAC_PROFILE;
            } else if (statusCode == cb::mcbp::Status::Etmpfail) {
                status = cb::sasl::Error::AUTH_PROVIDER_DIED;
            } else {
                status = cb::sasl::Error::FAIL;
            }

            if (!payload.empty()) {
                auto decoded = nlohmann::json::parse(payload);
                auto error = decoded["error"];
                auto context = error.find("context");
                if (context != error.end()) {
                    cookie.setErrorContext(context->get<std::string>());
                }
                auto ref = error.find("ref");
                if (ref != error.end()) {
                    cookie.setEventId(ref->get<std::string>());
                }
            }
        } catch (const std::exception& e) {
            LOG_WARNING_CTX("GetAuthorizationTask::externalResponse() failed",
                            {"conn_id", cookie.getConnectionId()},
                            {"event_id", cookie.getEventId()},
                            {"error", e.what()});
            status = cb::sasl::Error::FAIL;
        }
    }
    cookie.notifyIoComplete(cb::engine_errc::success);
}
