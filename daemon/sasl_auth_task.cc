/*
 *     Copyright 2018-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#include "sasl_auth_task.h"

#include "connection.h"
#include "cookie.h"
#include "external_auth_manager_thread.h"
#include "memcached.h"
#include "platform/timeutils.h"
#include "settings.h"
#include <cbsasl/server.h>
#include <logger/logger.h>
#include <mcbp/protocol/status.h>
#include <nlohmann/json.hpp>
#include <platform/base64.h>

using cb::mcbp::Status;
using cb::sasl::Error;

SaslAuthTask::SaslAuthTask(Cookie& cookie,
                           cb::sasl::server::ServerContext& serverContext,
                           std::string mechanism,
                           std::string challenge)
    : cookie(cookie),
      serverContext(serverContext),
      mechanism(std::move(mechanism)),
      challenge(std::move(challenge)),
      context(serverContext.getExternalServerContext()) {
    serverContext.setDomain(cb::sasl::Domain::External);
}

std::string SaslAuthTask::getUsername() const {
    return serverContext.getUsername();
}

nlohmann::json SaslAuthTask::getPeer() const {
    return cookie.getConnection().getPeername();
}

void SaslAuthTask::logIfSlowResponse() const {
    auto duration = steady_clock::now() - getStartTime();
    if (duration > externalAuthManager->getExternalAuthSlowDuration()) {
        LOG_WARNING_CTX("Slow external user authentication",
                        {"username", cb::tagUserData(getUsername())},
                        {"mechanism", mechanism},
                        {"duration", cb::time2text(duration)});
    }
}

void SaslAuthTask::externalResponse(Status status,
                                    const std::string_view payload) {
    logIfSlowResponse();
    try {
        nlohmann::json json;
        if (!payload.empty()) {
            json = nlohmann::json::parse(payload);
            if (json.contains("domain")) {
                if (json.value("domain", "external") == "local") {
                    serverContext.setDomain(cb::sasl::Domain::Local);
                }
            }
        }

        if (status == Status::Success) {
            successfull_external_auth(json);
        } else {
            unsuccessfull_external_auth(status, json);
        }
    } catch (const std::exception& exception) {
        LOG_WARNING_CTX("Exception occurred",
                        {"reason", exception.what()},
                        {"connection_id", cookie.getConnectionId()},
                        {"uuid", cookie.getEventId()});
        error = Error::FAIL;
    }

    cookie.notifyIoComplete(cb::engine_errc::success);
}

void SaslAuthTask::successfull_external_auth(const nlohmann::json& json) {
    error = Error::OK;
    if (json.contains("response")) {
        challenge = cb::base64::decode(json["response"].get<std::string>());
    } else {
        challenge.clear();
    }

    externalAuthManager->login(serverContext.getUsername());
}

void SaslAuthTask::unsuccessfull_external_auth(Status status,
                                               const nlohmann::json& json) {
    if (status == Status::AuthContinue) {
        error = Error::CONTINUE;
        context = json["context"];
        challenge = cb::base64::decode(json["response"].get<std::string>());
        return;
    }

    // The payload should contain an error message
    if (status == Status::AuthError) {
        error = Error::NO_RBAC_PROFILE;
    } else if (status == Status::AuthStale) {
        error = Error::PASSWORD_EXPIRED;
    } else if (status == Status::KeyEnoent) {
        error = Error::NO_USER;
    } else if (status == Status::KeyEexists) {
        error = Error::PASSWORD_ERROR;
    } else if (status == Status::Etmpfail) {
        error = Error::AUTH_PROVIDER_DIED;
    } else {
        error = Error::FAIL;
    }

    if (!json.empty() && json.contains("error")) {
        auto error = json["error"];
        if (error.contains("context")) {
            cookie.setErrorContext(error["context"]);
        }
        if (error.contains("ref")) {
            cookie.setEventId(error["ref"]);
        }
    }
}
