/*
 *     Copyright 2017-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "sasl_auth_command_context.h"

#include <daemon/buckets.h>
#include <daemon/connection.h>
#include <daemon/mcaudit.h>
#include <daemon/memcached.h>
#include <daemon/sasl_auth_task.h>
#include <daemon/settings.h>
#include <daemon/stats.h>
#include <logger/logger.h>
#include <utilities/logtags.h>

std::string getMechanism(std::string_view key) {
    std::string mechanism;
    // Uppercase the requested mechanism so that we don't have to remember
    // to do case insensitive comparisons all over the code
    std::transform(
            key.begin(), key.end(), std::back_inserter(mechanism), toupper);
    return mechanism;
}

SaslAuthCommandContext::SaslAuthCommandContext(Cookie& cookie)
    : SteppableCommandContext(cookie),
      request(cookie.getRequest()),
      mechanism(getMechanism(request.getKeyString())),
      challenge(request.getValueString()),
      state(State::Initial) {
}

cb::engine_errc SaslAuthCommandContext::tryHandleSaslOk(
        std::string_view payload) {
    auto& serverContext = *connection.getSaslServerContext();

    // Authentication successful, but it still has to be defined in
    // our system
    try {
        cb::rbac::createContext(serverContext.getUser(), {});
    } catch (const cb::rbac::NoSuchUserException&) {
        LOG_WARNING(
                "{}: User [{}] is not defined as a user in Couchbase. "
                "Mechanism:[{}], UUID:[{}]",
                connection.getId(),
                cb::UserDataView(serverContext.getUser().name),
                mechanism,
                cookie.getEventId());
        authFailure(cb::sasl::Error::NO_RBAC_PROFILE);
        return cb::engine_errc::success;
    }

    // Success
    connection.setAuthenticated(serverContext.getUser());
    audit_auth_success(connection, &cookie);
    auto& user = connection.getUser();
    LOG_INFO("{}: Client {} authenticated as {}. Mechanism:[{}]{}",
             connection.getId(),
             connection.getPeername().dump(),
             user.getSanitizedName(),
             mechanism,
             user.domain == cb::sasl::Domain::External ? " (LDAP)" : "");

    if (Settings::instance().isDeprecatedBucketAutoselectEnabled()) {
        // associate the connection with the appropriate bucket
        const auto username = connection.getUser().name;
        if (cookie.mayAccessBucket(username)) {
            associate_bucket(cookie, username);
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
    } else if (connection.getBucket().type == BucketType::NoBucket ||
               !cookie.mayAccessBucket(connection.getBucket().name)) {
        associate_bucket(cookie, "");
    }

    cookie.sendResponse(cb::mcbp::Status::Success,
                        {},
                        {},
                        payload,
                        cb::mcbp::Datatype::Raw,
                        0);
    stats.auth_cmds++;
    state = State::Done;
    return cb::engine_errc::success;
}

cb::engine_errc SaslAuthCommandContext::handleSaslAuthTaskResult() {
    if (task) {
        error = task->getError();
        payload = task->getChallenge();
        task->updateExternalAuthContext();
        task.reset();
    }

    const auto ret = doHandleSaslAuthTaskResult(error, payload);
    if (error != cb::sasl::Error::CONTINUE) {
        // we should _ONLY_ preserve the sasl server context if the underlying
        // sasl backend returns CONTINUE
        connection.releaseSaslServerContext();
    }
    return ret;
}

cb::engine_errc SaslAuthCommandContext::doHandleSaslAuthTaskResult(
        cb::sasl::Error error, std::string_view payload) {
    // If CBSASL generated a UUID, we should continue to use that UUID
    auto& serverContext = *connection.getSaslServerContext();
    if (serverContext.containsUuid()) {
        cookie.setEventId(serverContext.getUuid());
    }

    // Perform the appropriate logging for each error code
    switch (error) {
    case cb::sasl::Error::OK:
        return tryHandleSaslOk(payload);

    case cb::sasl::Error::CONTINUE:
        LOG_DEBUG("{}: SASL CONTINUE", connection.getId());
        return authContinue(payload);

    case cb::sasl::Error::BAD_PARAM:
        return authBadParameters();

    case cb::sasl::Error::FAIL:
    case cb::sasl::Error::NO_MEM:
        return authFailure(error);

    case cb::sasl::Error::NO_MECH:
        cookie.setErrorContext("Requested mechanism \"" + mechanism +
                               "\" is not supported");
        return authFailure(error);

    case cb::sasl::Error::NO_USER:
        LOG_WARNING("{}: User [{}] not found. Mechanism:[{}], UUID:[{}]",
                    connection.getId(),
                    cb::UserDataView(serverContext.getUser().name),
                    mechanism,
                    cookie.getEventId());
        audit_auth_failure(
                connection, serverContext.getUser(), "Unknown user", &cookie);
        return authFailure(error);

    case cb::sasl::Error::PASSWORD_ERROR:
        LOG_WARNING(
                "{}: Invalid password specified for [{}]. Mechanism:[{}], "
                "UUID:[{}]",
                connection.getId(),
                serverContext.getUser().getSanitizedName(),
                mechanism,
                cookie.getEventId());
        audit_auth_failure(connection,
                           serverContext.getUser(),
                           "Incorrect password",
                           &cookie);

        return authFailure(error);

    case cb::sasl::Error::NO_RBAC_PROFILE:
        LOG_WARNING(
                "{}: User [{}] is not defined as a user in Couchbase. "
                "Mechanism:[{}], UUID:[{}]",
                connection.getId(),
                cb::UserDataView(serverContext.getUser().name),
                mechanism,
                cookie.getEventId());
        audit_auth_failure(connection,
                           serverContext.getUser(),
                           "No RBAC profile",
                           &cookie);
        return authFailure(error);

    case cb::sasl::Error::AUTH_PROVIDER_DIED:
        LOG_WARNING("{}: Auth provider closed the connection. UUID:[{}]",
                    connection.getId(),
                    cookie.getEventId());
        return authFailure(error);
    }

    throw std::logic_error(
            "SaslAuthCommandContext::handleSaslAuthTaskResult: Unknown sasl "
            "error");
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

cb::engine_errc SaslAuthCommandContext::authContinue(
        std::string_view challenge) {
    cookie.sendResponse(cb::mcbp::Status::AuthContinue,
                        {},
                        {},
                        challenge,
                        cb::mcbp::Datatype::Raw,
                        0);
    state = State::Done;
    return cb::engine_errc::success;
}

cb::engine_errc SaslAuthCommandContext::authBadParameters() {
    stats.auth_cmds++;
    stats.auth_errors++;
    connection.releaseSaslServerContext();
    return cb::engine_errc::invalid_arguments;
}

cb::engine_errc SaslAuthCommandContext::authFailure(cb::sasl::Error error) {
    state = State::Done;
    if (error == cb::sasl::Error::AUTH_PROVIDER_DIED) {
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

    stats.auth_cmds++;
    stats.auth_errors++;

    connection.releaseSaslServerContext();
    return cb::engine_errc::success;
}
