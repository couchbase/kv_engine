/*
 *     Copyright 2017-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "sasl_refresh_command_context.h"

#include <cbsasl/mechanism.h>
#include <daemon/concurrency_semaphores.h>
#include <daemon/connection.h>
#include <daemon/settings.h>
#include <logger/logger.h>

SaslRefreshCommandContext::SaslRefreshCommandContext(Cookie& cookie)
    : BackgroundThreadCommandContext(
              cookie,
              TaskId::Core_SaslRefreshTask,
              "Refresh SASL database",
              ConcurrencySemaphores::instance().sasl_reload) {
}

cb::engine_errc SaslRefreshCommandContext::doSaslRefresh() {
    try {
        using namespace cb::sasl;
        switch (server::reload_password_database({})) {
        case Error::OK:
            LOG_INFO_RAW("Password database updated successfully");
            return cb::engine_errc::success;
        case Error::NO_MEM:
            LOG_WARNING_RAW(
                    "Failed to allocate memory to update password database");
            return cb::engine_errc::no_memory;
        case Error::FAIL:
            // Already logged
            return cb::engine_errc::failed;

        case Error::CONTINUE:
        case Error::BAD_PARAM:
        case Error::NO_MECH:
        case Error::NO_USER:
        case Error::PASSWORD_ERROR:
        case Error::PASSWORD_EXPIRED:
        case Error::NO_RBAC_PROFILE:
        case Error::AUTH_PROVIDER_DIED:
            break;
        }
        cookie.setErrorContext("Internal error");
        LOG_WARNING_CTX(
                "Internal error - Invalid return code from "
                "cb::sasl::server::refresh",
                {"conn_id", cookie.getConnectionId()},
                {"event_id", cookie.getEventId()});
    } catch (const std::exception& e) {
        std::string error = e.what();
        cookie.setErrorContext(e.what());
        LOG_WARNING_CTX("Failed to refresh password database",
                        {"conn_id", cookie.getConnectionId()},
                        {"error", error});
    } catch (...) {
        std::string error = "Unknown error";
        cookie.setErrorContext(error);
        LOG_WARNING_CTX("Failed to refresh password database",
                        {"conn_id", cookie.getConnectionId()},
                        {"error", error});
    }
    return cb::engine_errc::failed;
}

cb::engine_errc SaslRefreshCommandContext::execute() {
    try {
        return doSaslRefresh();
    } catch (const std::bad_alloc&) {
        return cb::engine_errc::no_memory;
    }
}
