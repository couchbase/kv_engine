/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2018-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#include "start_sasl_auth_task.h"

#include "connection.h"
#include "cookie.h"
#include "external_auth_manager_thread.h"
#include "memcached.h"
#include "settings.h"
#include <cbsasl/mechanism.h>
#include <cbsasl/server.h>
#include <logger/logger.h>
#include <nlohmann/json.hpp>

StartSaslAuthTask::StartSaslAuthTask(
        Cookie& cookie_,
        cb::sasl::server::ServerContext& serverContext_,
        std::string mechanism_,
        std::string challenge_)
    : cookie(cookie_),
      serverContext(serverContext_),
      mechanism(std::move(mechanism_)),
      challenge(std::move(challenge_)) {
    // no more init needed
}

std::string StartSaslAuthTask::getUsername() const {
    return serverContext.getUsername();
}

void StartSaslAuthTask::externalResponse(cb::mcbp::Status status,
                                         const std::string& payload) {
    if (status == cb::mcbp::Status::Success) {
        successfull_external_auth();
    } else {
        unsuccessfull_external_auth(status, payload);
    }

    ::notifyIoComplete(cookie, cb::engine_errc::success);
}

void StartSaslAuthTask::successfull_external_auth() {
    try {
        error = cb::sasl::Error::OK;
        externalAuthManager->login(serverContext.getUsername());
        serverContext.setDomain(cb::sasl::Domain::External);
    } catch (const std::exception& e) {
        LOG_WARNING(R"({} successfull_external_auth() failed. UUID[{}] "{}")",
                    cookie.getConnectionId(),
                    cookie.getEventId(),
                    e.what());
        error = cb::sasl::Error::FAIL;
    }
}

void StartSaslAuthTask::unsuccessfull_external_auth(
        cb::mcbp::Status status, const std::string& payload) {
    // The payload should contain an error message
    try {
        if (status == cb::mcbp::Status::AuthError) {
            error = cb::sasl::Error::NO_RBAC_PROFILE;
        } else if (status == cb::mcbp::Status::KeyEnoent) {
            error = cb::sasl::Error::NO_USER;
        } else if (status == cb::mcbp::Status::KeyEexists) {
            error = cb::sasl::Error::PASSWORD_ERROR;
        } else if (status == cb::mcbp::Status::Etmpfail) {
            error = cb::sasl::Error::AUTH_PROVIDER_DIED;
        } else {
            error = cb::sasl::Error::FAIL;
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
        LOG_WARNING(R"({} successfull_external_auth() failed. UUID[{}] "{}")",
                    cookie.getConnectionId(),
                    cookie.getEventId(),
                    e.what());
        error = cb::sasl::Error::FAIL;
    }
}
