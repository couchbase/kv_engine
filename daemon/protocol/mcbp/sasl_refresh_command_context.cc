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

#include "sasl_refresh_command_context.h"

#include <cbsasl/mechanism.h>
#include <daemon/connection.h>
#include <daemon/cookie.h>
#include <daemon/runtime.h>
#include <logger/logger.h>
#include <platform/platform_thread.h>

static void cbsasl_refresh_main(void* void_cookie) {
    auto& cookie = *reinterpret_cast<Cookie*>(void_cookie);
    ENGINE_ERROR_CODE rv = ENGINE_SUCCESS;
    std::string error;
    try {
        using namespace cb::sasl;
        switch (server::refresh()) {
        case Error::OK:
            rv = ENGINE_SUCCESS;
            set_default_bucket_enabled(
                    mechanism::plain::authenticate("default", "") == Error::OK);
            break;
        case Error::NO_MEM:
            rv = ENGINE_ENOMEM;
            break;
        case Error::FAIL:
            rv = ENGINE_FAILED;
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
                    "{}: {} - Internal error - Invalid return code from "
                    "cb::sasl::server::refresh()",
                    cookie.getConnection().getId(),
                    cookie.getEventId());
            rv = ENGINE_FAILED;
        }
    } catch (const std::exception& e) {
        rv = ENGINE_FAILED;
        error = e.what();
        cookie.setErrorContext(error);
        LOG_WARNING("{}: Failed to refresh password database: {}",
                    cookie.getConnection().getId(),
                    error);
    } catch (...) {
        rv = ENGINE_FAILED;
        error = "Unknown error";
        cookie.setErrorContext(error);
        LOG_WARNING("{}: Failed to refresh password database: {}",
                    cookie.getConnection().getId(),
                    error);
    }

    ::notifyIoComplete(cookie, rv);
}

ENGINE_ERROR_CODE SaslRefreshCommandContext::refresh() {
    state = State::Done;

    cb_thread_t tid;
    auto status = cb_create_named_thread(&tid,
                                         cbsasl_refresh_main,
                                         static_cast<void*>(&cookie),
                                         1,
                                         "mc:refresh_sasl");
    if (status != 0) {
        cookie.setErrorContext("Failed to create cbsasl db update thread");
        LOG_WARNING("{}: {}: {}",
                    connection.getId(),
                    cookie.getErrorContext(),
                    strerror(status));
        return ENGINE_TMPFAIL;
    }

    return ENGINE_EWOULDBLOCK;
}

void SaslRefreshCommandContext::done() {
    cookie.sendResponse(cb::mcbp::Status::Success);
}
