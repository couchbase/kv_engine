/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2018 Couchbase, Inc.
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
#include "start_sasl_auth_task.h"

#include "connection.h"
#include "cookie.h"
#include "external_auth_manager_thread.h"
#include "settings.h"
#include <cbsasl/mechanism.h>
#include <cbsasl/server.h>
#include <logger/logger.h>
#include <nlohmann/json.hpp>

StartSaslAuthTask::StartSaslAuthTask(Cookie& cookie_,
                                     Connection& connection_,
                                     const std::string& mechanism_,
                                     const std::string& challenge_)
    : SaslAuthTask(cookie_, connection_, mechanism_, challenge_) {
    // No extra initialization needed
}

Task::Status StartSaslAuthTask::execute() {
    if (internal) {
        return internal_auth();
    }

    return external_auth();
}

Task::Status StartSaslAuthTask::internal_auth() {
    connection.restartAuthentication();
    auto& server = serverContext;

    try {
        if (connection.isSslEnabled()) {
            response = server.start(mechanism,
                                    Settings::instance().getSslSaslMechanisms(),
                                    challenge);
        } else {
            response = server.start(mechanism,
                                    Settings::instance().getSaslMechanisms(),
                                    challenge);
        }
    } catch (const cb::sasl::unknown_mechanism&) {
        response.first = cb::sasl::Error::NO_MECH;
    } catch (const std::bad_alloc&) {
        LOG_WARNING("{}: StartSaslAuthTask::execute(): std::bad_alloc",
                    connection.getId());
        response.first = cb::sasl::Error::NO_MEM;
    } catch (const std::exception& exception) {
        // If we generated an error as part of SASL, we should
        // return that back to the client
        if (server.containsUuid()) {
            cookie.setEventId(server.getUuid());
        }
        LOG_WARNING(
                "{}: StartSaslAuthTask::execute(): UUID:[{}] An exception "
                "occurred: {}",
                connection.getId(),
                cookie.getEventId(),
                exception.what());
        cookie.setErrorContext("An exception occurred");
        response.first = cb::sasl::Error::FAIL;
    }

    if (response.first == cb::sasl::Error::NO_USER &&
        Settings::instance().isExternalAuthServiceEnabled() &&
        mechanism == "PLAIN") {
        // We can't hold this lock when we're trying to enqueue the
        // request
        internal = false;
        externalAuthManager->enqueueRequest(*this);
    }

    return internal ? Status::Finished : Status::Continue;
}

Task::Status StartSaslAuthTask::external_auth() {
    return Status::Finished;
}

std::string StartSaslAuthTask::getUsername() const {
    return connection.getSaslConn().getUsername();
}

void StartSaslAuthTask::externalResponse(cb::mcbp::Status status,
                                         const std::string& payload) {
    std::lock_guard<std::mutex> guard(getMutex());

    if (status == cb::mcbp::Status::Success) {
        successfull_external_auth();
    } else {
        unsuccessfull_external_auth(status, payload);
    }

    makeRunnable();
}

void StartSaslAuthTask::successfull_external_auth() {
    try {
        response.first = cb::sasl::Error::OK;
        externalAuthManager->login(serverContext.getUsername());
        serverContext.setDomain(cb::sasl::Domain::External);
    } catch (const std::exception& e) {
        LOG_WARNING(R"({} successfull_external_auth() failed. UUID[{}] "{}")",
                    connection.getId(),
                    cookie.getEventId(),
                    e.what());
        response.first = cb::sasl::Error::FAIL;
    }
}

void StartSaslAuthTask::unsuccessfull_external_auth(
        cb::mcbp::Status status, const std::string& payload) {
    // The paylaod should contain an error message
    try {
        if (status == cb::mcbp::Status::AuthError) {
            response.first = cb::sasl::Error::NO_RBAC_PROFILE;
        } else if (status == cb::mcbp::Status::KeyEnoent) {
            response.first = cb::sasl::Error::NO_USER;
        } else if (status == cb::mcbp::Status::KeyEexists) {
            response.first = cb::sasl::Error::PASSWORD_ERROR;
        } else if (status == cb::mcbp::Status::Etmpfail) {
            response.first = cb::sasl::Error::AUTH_PROVIDER_DIED;
        } else {
            response.first = cb::sasl::Error::FAIL;
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
                    connection.getId(),
                    cookie.getEventId(),
                    e.what());
        response.first = cb::sasl::Error::FAIL;
    }
}
