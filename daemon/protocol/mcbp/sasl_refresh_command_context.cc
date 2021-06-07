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

#include "sasl_refresh_command_context.h"

#include <cbsasl/mechanism.h>
#include <daemon/connection.h>
#include <daemon/one_shot_task.h>
#include <daemon/runtime.h>
#include <executor/executorpool.h>
#include <logger/logger.h>

cb::engine_errc SaslRefreshCommandContext::refresh() {
    state = State::Done;

    ExecutorPool::get()->schedule(std::make_shared<OneShotTask>(
            TaskId::Core_SaslRefreshTask,
            "Refresh SASL database",
            [client = &cookie]() {
                auto& cookie = *client;
                cb::engine_errc rv = cb::engine_errc::success;
                try {
                    using namespace cb::sasl;
                    switch (server::refresh()) {
                    case Error::OK:
                        rv = cb::engine_errc::success;
                        set_default_bucket_enabled(
                                mechanism::plain::authenticate("default", "") ==
                                Error::OK);
                        break;
                    case Error::NO_MEM:
                        rv = cb::engine_errc::no_memory;
                        break;
                    case Error::FAIL:
                        rv = cb::engine_errc::failed;
                        break;

                    case Error::CONTINUE:
                    case Error::BAD_PARAM:
                    case Error::NO_MECH:
                    case Error::NO_USER:
                    case Error::PASSWORD_ERROR:
                    case Error::NO_RBAC_PROFILE:
                    case Error::AUTH_PROVIDER_DIED:
                        cookie.setErrorContext("Internal error");
                        LOG_WARNING(
                                "{}: {} - Internal error - Invalid return code "
                                "from "
                                "cb::sasl::server::refresh()",
                                cookie.getConnection().getId(),
                                cookie.getEventId());
                        rv = cb::engine_errc::failed;
                    }
                } catch (const std::exception& e) {
                    rv = cb::engine_errc::failed;
                    std::string error = e.what();
                    cookie.setErrorContext(e.what());
                    LOG_WARNING("{}: Failed to refresh password database: {}",
                                cookie.getConnection().getId(),
                                error);
                } catch (...) {
                    rv = cb::engine_errc::failed;
                    std::string error = "Unknown error";
                    cookie.setErrorContext(error);
                    LOG_WARNING("{}: Failed to refresh password database: {}",
                                cookie.getConnection().getId(),
                                error);
                }

                ::notifyIoComplete(cookie, rv);
            }));

    return cb::engine_errc::would_block;
}

void SaslRefreshCommandContext::done() {
    cookie.sendResponse(cb::mcbp::Status::Success);
}
