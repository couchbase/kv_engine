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
#include "step_sasl_auth_task.h"

#include "connection.h"
#include "cookie.h"

#include <logger/logger.h>

StepSaslAuthTask::StepSaslAuthTask(Cookie& cookie_,
                                   Connection& connection_,
                                   const std::string& mechanism_,
                                   const std::string& challenge_)
    : SaslAuthTask(cookie_, connection_, mechanism_, challenge_) {
    // No extra initialization needed
}

Task::Status StepSaslAuthTask::execute() {
    try {
        response = serverContext.step(challenge);
    } catch (const std::bad_alloc&) {
        LOG_WARNING("{}: StepSaslAuthTask::execute(): std::bad_alloc",
                    connection.getId());
        response.first = cb::sasl::Error::NO_MEM;
    } catch (const std::exception& exception) {
        // If we generated an error as part of SASL, we should
        // return that back to the client
        if (serverContext.containsUuid()) {
            cookie.setEventId(serverContext.getUuid());
        }
        LOG_WARNING(
                "{}: StepSaslAuthTask::execute(): UUID:[{}] An exception "
                "occurred: {}",
                connection.getId(),
                cookie.getEventId(),
                exception.what());
        cookie.setErrorContext("An exception occurred");
        response.first = cb::sasl::Error::FAIL;
    }
    return Status::Finished;
}

void StepSaslAuthTask::externalResponse(cb::mcbp::Status, const std::string&) {
    throw std::runtime_error(
            "StepSaslAuthTask::externalResponse(): multiple roundtrips to the "
            "external service is not implemented");
}
