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
#include <cbsasl/user.h>
#include <daemon/connection.h>
#include <daemon/one_shot_task.h>
#include <daemon/settings.h>
#include <executor/executorpool.h>
#include <logger/logger.h>

cb::engine_errc SaslRefreshCommandContext::doSaslRefresh() {
    try {
        using namespace cb::sasl;
        switch (server::reload_password_database({})) {
        case Error::OK:
            return cb::engine_errc::success;
        case Error::NO_MEM:
            return cb::engine_errc::no_memory;
        case Error::FAIL:
            return cb::engine_errc::failed;

        case Error::CONTINUE:
        case Error::BAD_PARAM:
        case Error::NO_MECH:
        case Error::NO_USER:
        case Error::PASSWORD_ERROR:
        case Error::NO_RBAC_PROFILE:
        case Error::AUTH_PROVIDER_DIED:
            break;
        }
        cookie.setErrorContext("Internal error");
        LOG_WARNING(
                "{}: {} - Internal error - Invalid return code from "
                "cb::sasl::server::refresh()",
                cookie.getConnectionId(),
                cookie.getEventId());
    } catch (const std::exception& e) {
        std::string error = e.what();
        cookie.setErrorContext(e.what());
        LOG_WARNING("{}: Failed to refresh password database: {}",
                    cookie.getConnectionId(),
                    error);
    } catch (...) {
        std::string error = "Unknown error";
        cookie.setErrorContext(error);
        LOG_WARNING("{}: Failed to refresh password database: {}",
                    cookie.getConnectionId(),
                    error);
    }
    return cb::engine_errc::failed;
}

cb::engine_errc SaslRefreshCommandContext::reload() {
    ExecutorPool::get()->schedule(std::make_shared<OneShotTask>(
            TaskId::Core_SaslRefreshTask, "Refresh SASL database", [this]() {
                try {
                    ::notifyIoComplete(cookie, doSaslRefresh());
                } catch (const std::bad_alloc&) {
                    ::notifyIoComplete(cookie, cb::engine_errc::no_memory);
                }
            }));

    return cb::engine_errc::would_block;
}
