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

#include "sasl_auth_command_context.h"

#include <daemon/connection.h>
#include <daemon/executorpool.h>
#include <daemon/mcaudit.h>
#include <daemon/memcached.h>
#include <daemon/settings.h>
#include <daemon/start_sasl_auth_task.h>
#include <daemon/stats.h>
#include <daemon/step_sasl_auth_task.h>
#include <logger/logger.h>

cb::engine_errc SaslAuthCommandContext::initial() {
    if (!connection.isSaslAuthEnabled()) {
        return cb::engine_errc::not_supported;
    }

    // Uppercase the requested mechanism so that we don't have to remember
    // to do case insensitive comparisons all over the code
    auto k = request.getKey();
    const auto key =
            std::string_view{reinterpret_cast<const char*>(k.data()), k.size()};
    std::string mechanism;
    std::transform(
            key.begin(), key.end(), std::back_inserter(mechanism), toupper);

    auto v = request.getValue();
    std::string challenge(reinterpret_cast<const char*>(v.data()), v.size());

    LOG_DEBUG("{}: SASL auth with mech: '{}' with {} bytes of data",
              connection.getId(),
              mechanism,
              v.size());

    if (request.getClientOpcode() == cb::mcbp::ClientOpcode::SaslAuth) {
        if (connection.getSaslServerContext()) {
            cookie.setErrorContext(
                    R"(Logic error: The server expects SASL STEP after returning CONTINUE from SASL START)");
            return cb::engine_errc::invalid_arguments;
        }

        connection.createSaslServerContext();
        connection.restartAuthentication();
        task = std::make_shared<StartSaslAuthTask>(
                cookie, connection, mechanism, challenge);
    } else if (request.getClientOpcode() == cb::mcbp::ClientOpcode::SaslStep) {
        if (!connection.getSaslServerContext()) {
            cookie.setErrorContext(
                    R"(Logic error: The server expects the client to start with SASL_START before sending SASL STEP)");
            return cb::engine_errc::invalid_arguments;
        }
        task = std::make_shared<StepSaslAuthTask>(
                cookie, connection, mechanism, challenge);
    } else {
        throw std::logic_error(
            "SaslAuthCommandContext() used with illegal opcode");
    }

    std::lock_guard<std::mutex> guard(task->getMutex());
    executorPool->schedule(task, true);

    state = State::ParseAuthTaskResult;
    return cb::engine_errc::would_block;
}

cb::engine_errc SaslAuthCommandContext::parseAuthTaskResult() {
    auto auth_task = reinterpret_cast<SaslAuthTask*>(task.get());

    switch (auth_task->getError()) {
    case cb::sasl::Error::OK:
        state = State::AuthOk;
        return cb::engine_errc::success;

    case cb::sasl::Error::CONTINUE:
        state = State::AuthContinue;
        return cb::engine_errc::success;

    case cb::sasl::Error::BAD_PARAM:
        state = State::AuthBadParameters;
        return cb::engine_errc::success;

    case cb::sasl::Error::FAIL:
    case cb::sasl::Error::NO_MEM:
    case cb::sasl::Error::NO_MECH:
    case cb::sasl::Error::NO_USER:
    case cb::sasl::Error::PASSWORD_ERROR:
    case cb::sasl::Error::NO_RBAC_PROFILE:
    case cb::sasl::Error::AUTH_PROVIDER_DIED:
        state = State::AuthFailure;
        return cb::engine_errc::success;
    }
    throw std::logic_error(
            "SaslAuthCommandContext::parseAuthTaskResult: Unknown sasl error");
}

cb::engine_errc SaslAuthCommandContext::step() {
    auto ret = cb::engine_errc::success;
    do {
        switch (state) {
        case State::Initial:
            ret = initial();
            break;
        case State::ParseAuthTaskResult:
            ret = parseAuthTaskResult();
            break;
        case State::AuthOk:
            ret = authOk();
            break;
        case State::AuthContinue:
            ret = authContinue();
            break;
        case State::AuthBadParameters:
            ret = authBadParameters();
            break;
        case State::AuthFailure:
            ret = authFailure();
            break;

        case State::Done:
            return cb::engine_errc::success;
        }
    } while (ret == cb::engine_errc::success);

    return ret;
}

cb::engine_errc SaslAuthCommandContext::authOk() {
    auto auth_task = reinterpret_cast<SaslAuthTask*>(task.get());
    auto payload = auth_task->getResponse();
    cookie.sendResponse(cb::mcbp::Status::Success,
                        {},
                        {},
                        payload,
                        cb::mcbp::Datatype::Raw,
                        0);
    get_thread_stats(&connection)->auth_cmds++;
    state = State::Done;
    connection.releaseSaslServerContext();
    return cb::engine_errc::success;
}

cb::engine_errc SaslAuthCommandContext::authContinue() {
    auto auth_task = reinterpret_cast<SaslAuthTask*>(task.get());
    auto payload = auth_task->getResponse();
    cookie.sendResponse(cb::mcbp::Status::AuthContinue,
                        {},
                        {},
                        payload,
                        cb::mcbp::Datatype::Raw,
                        0);
    state = State::Done;
    return cb::engine_errc::success;
}

cb::engine_errc SaslAuthCommandContext::authBadParameters() {
    auto* ts = get_thread_stats(&connection);
    ts->auth_cmds++;
    ts->auth_errors++;
    connection.releaseSaslServerContext();
    return cb::engine_errc::invalid_arguments;
}

cb::engine_errc SaslAuthCommandContext::authFailure() {
    state = State::Done;

    auto auth_task = reinterpret_cast<SaslAuthTask*>(task.get());
    if (auth_task->getError() == cb::sasl::Error::NO_USER ||
        auth_task->getError() == cb::sasl::Error::PASSWORD_ERROR) {
        audit_auth_failure(connection,
                           auth_task->getUser(),
                           auth_task->getError() == cb::sasl::Error::NO_USER
                                   ? "Unknown user"
                                   : "Incorrect password");
    }

    if (auth_task->getError() == cb::sasl::Error::AUTH_PROVIDER_DIED) {
        cookie.sendResponse(cb::mcbp::Status::Etmpfail);
    } else {
        if (Settings::instance().isExternalAuthServiceEnabled()) {
            cookie.setErrorContext(
                    "Authentication failed. This could be due to invalid "
                    "credentials or if the user is an external user the "
                    "external authentication service may not support the "
                    "selected authentication mechanism.");
        }
        cookie.sendResponse(cb::mcbp::Status::AuthError);
    }

    auto* ts = get_thread_stats(&connection);
    ts->auth_cmds++;
    ts->auth_errors++;

    connection.releaseSaslServerContext();
    return cb::engine_errc::success;
}
