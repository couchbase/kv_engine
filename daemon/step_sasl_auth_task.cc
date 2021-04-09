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
#include "step_sasl_auth_task.h"

#include "connection.h"
#include "cookie.h"

#include <logger/logger.h>

StepSaslAuthTask::StepSaslAuthTask(
        Cookie& cookie_,
        cb::sasl::server::ServerContext& serverContext_,
        const std::string& mechanism_,
        const std::string& challenge_)
    : SaslAuthTask(cookie_, serverContext_, mechanism_, challenge_) {
    // No extra initialization needed
}

Task::Status StepSaslAuthTask::execute() {
    try {
        response = serverContext.step(challenge);
    } catch (const std::bad_alloc&) {
        LOG_WARNING("{}: StepSaslAuthTask::execute(): std::bad_alloc",
                    cookie.getConnection().getId());
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
                cookie.getConnection().getId(),
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
