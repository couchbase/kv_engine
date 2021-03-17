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

#include <daemon/buckets.h>
#include <daemon/connection.h>
#include <daemon/executorpool.h>
#include <daemon/mcaudit.h>
#include <daemon/memcached.h>
#include <daemon/settings.h>
#include <daemon/start_sasl_auth_task.h>
#include <daemon/stats.h>
#include <daemon/step_sasl_auth_task.h>
#include <logger/logger.h>
#include <utilities/logtags.h>

std::string getMechanism(cb::const_byte_buffer k) {
    std::string mechanism;
    const auto key =
            std::string_view{reinterpret_cast<const char*>(k.data()), k.size()};
    // Uppercase the requested mechanism so that we don't have to remember
    // to do case insensitive comparisons all over the code
    std::transform(
            key.begin(), key.end(), std::back_inserter(mechanism), toupper);
    return mechanism;
}

SaslAuthCommandContext::SaslAuthCommandContext(Cookie& cookie)
    : SteppableCommandContext(cookie),
      request(cookie.getRequest()),
      mechanism(getMechanism(request.getKey())),
      state(State::Initial) {
}

cb::engine_errc SaslAuthCommandContext::initial() {
    if (!connection.isSaslAuthEnabled()) {
        return cb::engine_errc::not_supported;
    }

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
                cookie,
                *connection.getSaslServerContext(),
                mechanism,
                challenge);
    } else if (request.getClientOpcode() == cb::mcbp::ClientOpcode::SaslStep) {
        if (!connection.getSaslServerContext()) {
            cookie.setErrorContext(
                    R"(Logic error: The server expects the client to start with SASL_START before sending SASL STEP)");
            return cb::engine_errc::invalid_arguments;
        }
        task = std::make_shared<StepSaslAuthTask>(
                cookie,
                *connection.getSaslServerContext(),
                mechanism,
                challenge);
    } else {
        throw std::logic_error(
            "SaslAuthCommandContext() used with illegal opcode");
    }

    std::lock_guard<std::mutex> guard(task->getMutex());
    executorPool->schedule(task, true);

    state = State::HandleSaslAuthTaskResult;
    return cb::engine_errc::would_block;
}

cb::engine_errc SaslAuthCommandContext::tryHandleSaslOk() {
    auto& serverContext = *connection.getSaslServerContext();
    auto auth_task = reinterpret_cast<SaslAuthTask*>(task.get());
    std::pair<cb::rbac::PrivilegeContext, bool> context{
            cb::rbac::PrivilegeContext{cb::sasl::Domain::Local}, false};

    // Authentication successful, but it still has to be defined in
    // our system
    try {
        context = cb::rbac::createInitialContext(serverContext.getUser());
    } catch (const cb::rbac::NoSuchUserException&) {
        LOG_WARNING(
                "{}: User [{}] is not defined as a user in Couchbase. "
                "Mechanism:[{}], UUID:[{}]",
                connection.getId(),
                cb::UserDataView(serverContext.getUser().name),
                mechanism,
                cookie.getEventId());
        authFailure();
        return cb::engine_errc::success;
    }

    // Success
    connection.setAuthenticated(true, context.second, serverContext.getUser());
    audit_auth_success(connection, &cookie);
    LOG_INFO("{}: Client {} authenticated as {}",
             connection.getId(),
             connection.getPeername(),
             cb::UserDataView(connection.getUser().name));

    /* associate the connection with the appropriate bucket */
    {
        std::string username = connection.getUser().name;
        auto idx = username.find(";legacy");
        if (idx != username.npos) {
            username.resize(idx);
        }

        if (mayAccessBucket(cookie, username)) {
            associate_bucket(cookie, username.c_str());
            // Auth succeeded but the connection may not be valid for the
            // bucket
            if (connection.isCollectionsSupported() &&
                !connection.getBucket().supports(
                        cb::engine::Feature::Collections)) {
                // Move back to the "no bucket" as this is not valid
                associate_bucket(cookie, "");
            }
        } else {
            // the user don't have access to that bucket, move the
            // connection to the "no bucket"
            associate_bucket(cookie, "");
        }
    }

    auto payload = auth_task->getResponse();
    cookie.sendResponse(cb::mcbp::Status::Success,
                        {},
                        {},
                        payload,
                        cb::mcbp::Datatype::Raw,
                        0);
    get_thread_stats(&connection)->auth_cmds++;
    state = State::Done;
    return cb::engine_errc::success;
}

cb::engine_errc SaslAuthCommandContext::doHandleSaslAuthTaskResult(
        SaslAuthTask* auth_task) {
    auto& serverContext = *connection.getSaslServerContext();

    // If CBSASL generated a UUID, we should continue to use that UUID
    if (serverContext.containsUuid()) {
        cookie.setEventId(serverContext.getUuid());
    }

    // Perform the appropriate logging for each error code
    switch (auth_task->getError()) {
    case cb::sasl::Error::OK:
        return tryHandleSaslOk();

    case cb::sasl::Error::CONTINUE:
        LOG_DEBUG("{}: SASL CONTINUE", connection.getId());
        return authContinue();

    case cb::sasl::Error::BAD_PARAM:
        return authBadParameters();

    case cb::sasl::Error::FAIL:
    case cb::sasl::Error::NO_MEM:
        return authFailure();

    case cb::sasl::Error::NO_MECH:
        cookie.setErrorContext("Requested mechanism \"" + mechanism +
                               "\" is not supported");
        return authFailure();

    case cb::sasl::Error::NO_USER:
        LOG_WARNING("{}: User [{}] not found. Mechanism:[{}], UUID:[{}]",
                    connection.getId(),
                    cb::UserDataView(connection.getUser().name),
                    mechanism,
                    cookie.getEventId());
        audit_auth_failure(
                connection, serverContext.getUser(), "Unknown user", &cookie);
        return authFailure();

    case cb::sasl::Error::PASSWORD_ERROR:
        LOG_WARNING(
                "{}: Invalid password specified for [{}]. Mechanism:[{}], "
                "UUID:[{}]",
                connection.getId(),
                cb::UserDataView(connection.getUser().name),
                mechanism,
                cookie.getEventId());
        audit_auth_failure(connection,
                           serverContext.getUser(),
                           "Incorrect password",
                           &cookie);

        return authFailure();

    case cb::sasl::Error::NO_RBAC_PROFILE:
        LOG_WARNING(
                "{}: User [{}] is not defined as a user in Couchbase. "
                "Mechanism:[{}], UUID:[{}]",
                connection.getId(),
                cb::UserDataView(connection.getUser().name),
                mechanism,
                cookie.getEventId());
        return authFailure();

    case cb::sasl::Error::AUTH_PROVIDER_DIED:
        LOG_WARNING("{}: Auth provider closed the connection. UUID:[{}]",
                    connection.getId(),
                    cookie.getEventId());
        return authFailure();
    }

    throw std::logic_error(
            "SaslAuthCommandContext::handleSaslAuthTaskResult: Unknown sasl "
            "error");
}

cb::engine_errc SaslAuthCommandContext::handleSaslAuthTaskResult() {
    auto auth_task = reinterpret_cast<SaslAuthTask*>(task.get());
    auto error = auth_task->getError();
    auto ret = doHandleSaslAuthTaskResult(auth_task);
    if (error != cb::sasl::Error::CONTINUE) {
        // we should _ONLY_ preserve the sasl server context if the underlying
        // sasl backend returns CONTINUE
        connection.releaseSaslServerContext();
    }
    return ret;
}

cb::engine_errc SaslAuthCommandContext::step() {
    auto ret = cb::engine_errc::success;
    do {
        switch (state) {
        case State::Initial:
            ret = initial();
            break;
        case State::HandleSaslAuthTaskResult:
            ret = handleSaslAuthTaskResult();
            break;
        case State::Done:
            // All done and we've sent a response to the client
            return cb::engine_errc::success;
        }
    } while (ret == cb::engine_errc::success);

    return ret;
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
