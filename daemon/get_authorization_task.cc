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

void GetAuthorizationTask::externalResponse(cb::mcbp::Status statusCode,
                                            const std::string& payload) {
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
            LOG_WARNING(
                    R"({} GetAuthorizationTask::externalResponse() failed. UUID[{}] "{}")",
                    cookie.getConnection().getId(),
                    cookie.getEventId(),
                    e.what());
            status = cb::sasl::Error::FAIL;
        }
    }
    ::notifyIoComplete(cookie, cb::engine_errc::success);
}
