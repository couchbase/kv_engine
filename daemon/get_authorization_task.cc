/*
 *     Copyright 2020 Couchbase, Inc.
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

#include "get_authorization_task.h"
#include "connection.h"
#include "cookie.h"
#include "external_auth_manager_thread.h"
#include "log_macros.h"
#include "memcached.h"

void GetAuthorizationTask::externalResponse(cb::mcbp::Status statusCode,
                                            const std::string& payload) {
    std::lock_guard<std::mutex> guard(getMutex());

    if (statusCode == cb::mcbp::Status::Success) {
        status = cb::sasl::Error::OK;
    } else {
        try {
            if (statusCode == cb::mcbp::Status::AuthError) {
                status = cb::sasl::Error::NO_RBAC_PROFILE;
            } else if (statusCode == cb::mcbp::Status::Etmpfail) {
                status = cb::sasl::Error::AUTH_PROVIDER_DIED;
            } else {
                status = cb::sasl::Error::FAIL;
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
            LOG_WARNING(
                    R"({} GetAuthorizationTask::externalResponse() failed. UUID[{}] "{}")",
                    cookie.getConnection().getId(),
                    cookie.getEventId(),
                    e.what());
            status = cb::sasl::Error::FAIL;
        }
    }

    makeRunnable();
}

Task::Status GetAuthorizationTask::execute() {
    if (!requestSent) {
        externalAuthManager->enqueueRequest(*this);
        requestSent = true;
        return Status::Continue;
    }

    return Status::Finished;
}

void GetAuthorizationTask::notifyExecutionComplete() {
    ::notifyIoComplete(cookie, ENGINE_SUCCESS);
}
