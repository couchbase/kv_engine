/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2016 Couchbase, Inc.
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
#include "config.h"
#include "sasl_tasks.h"
#include "mcaudit.h"
#include "memcached.h"
#include <memcached/rbac.h>
#include <utilities/logtags.h>

StartSaslAuthTask::StartSaslAuthTask(Cookie& cookie_,
                                     Connection& connection_,
                                     const std::string& mechanism_,
                                     const std::string& challenge_)
    : SaslAuthTask(cookie_, connection_, mechanism_, challenge_) {
    // No extra initialization needed
}

Task::Status StartSaslAuthTask::execute() {
    connection.restartAuthentication();
    try {
        error = cbsasl_server_start(connection.getSaslConn(),
                                    mechanism.c_str(),
                                    challenge.data(),
                                    static_cast<unsigned int>(challenge.length()),
                                    &response, &response_length);
    } catch (const std::bad_alloc&) {
        LOG_WARNING("{}: StartSaslAuthTask::execute(): std::bad_alloc",
                    connection.getId());
        error = CBSASL_NOMEM;
    } catch (const std::exception& exception) {
        // If we generated an error as part of SASL, we should
        // return that back to the client
        auto& uuid = cb::sasl::get_uuid(connection.getSaslConn());
        if (!uuid.empty()) {
            cookie.setEventId(uuid);
        }
        LOG_WARNING(
                "{}: StartSaslAuthTask::execute(): UUID:[{}] An exception "
                "occurred: {}",
                connection.getId(),
                cookie.getEventId(),
                exception.what());
        cookie.setErrorContext("An exception occurred");
        error = CBSASL_FAIL;
    }

    return Status::Finished;
}

StepSaslAuthTask::StepSaslAuthTask(Cookie& cookie_,
                                   Connection& connection_,
                                   const std::string& mechanism_,
                                   const std::string& challenge_)
    : SaslAuthTask(cookie_, connection_, mechanism_, challenge_) {
    // No extra initialization needed
}

Task::Status StepSaslAuthTask::execute() {
    try {
        error = cbsasl_server_step(connection.getSaslConn(), challenge.data(),
                                   static_cast<unsigned int>(challenge.length()),
                                   &response, &response_length);
    } catch (const std::bad_alloc&) {
        LOG_WARNING("{}: StepSaslAuthTask::execute(): std::bad_alloc",
                    connection.getId());
        error = CBSASL_NOMEM;
    } catch (const std::exception& exception) {
        // If we generated an error as part of SASL, we should
        // return that back to the client
        auto& uuid = cb::sasl::get_uuid(connection.getSaslConn());
        if (!uuid.empty()) {
            cookie.setEventId(uuid);
        }
        LOG_WARNING(
                "{}: StepSaslAuthTask::execute(): UUID:[{}] An exception "
                "occurred: {}",
                connection.getId(),
                cookie.getEventId(),
                exception.what());
        cookie.setErrorContext("An exception occurred");
        error = CBSASL_FAIL;
    }
    return Status::Finished;
}


SaslAuthTask::SaslAuthTask(Cookie& cookie_,
                           Connection& connection_,
                           const std::string& mechanism_,
                           const std::string& challenge_)
    : cookie(cookie_),
      connection(connection_),
      mechanism(mechanism_),
      challenge(challenge_),
      error(CBSASL_FAIL),
      response(nullptr),
      response_length(0) {
    // no more init needed
}

void SaslAuthTask::notifyExecutionComplete() {
    connection.setAuthenticated(false);
    std::pair<cb::rbac::PrivilegeContext, bool> context;

    // If CBSASL generated a UUID, we should continue to use that UUID
    auto& uuid = cb::sasl::get_uuid(connection.getSaslConn());
    if (!uuid.empty()) {
        cookie.setEventId(uuid);
    }

    if (error == CBSASL_OK) {
        // Authentication successful, but it still has to be defined in
        // our system
        try {
            context = cb::rbac::createInitialContext(connection.getUsername(),
                                                     connection.getDomain());
        } catch (const cb::rbac::NoSuchUserException&) {
            error = CBSASL_NO_RBAC_PROFILE;
        }
    }

    // Perform the appropriate logging for each error code
    switch (error) {
    case CBSASL_OK:
        break;
    case CBSASL_CONTINUE:
        LOG_DEBUG("{}: SASL CONTINUE", connection.getId());
        break;
    case CBSASL_FAIL:
        // Should already have been logged
        break;
    case CBSASL_NOMEM:
        // Should already have been logged
        break;
    case CBSASL_BADPARAM:
        // May have been logged by cbsasl
        break;
    case CBSASL_NOMECH:
        // Should already have been logged
        break;
    case CBSASL_NOUSER:
        LOG_WARNING("{}: User [{}] not found. UUID:[{}]",
                    connection.getId(),
                    cb::logtags::tagUserData(connection.getUsername()),
                    cookie.getEventId());
        break;
    case CBSASL_PWERR:
        LOG_WARNING("{}: Invalid password specified for [{}] UUID:[{}]",
                    connection.getId(),
                    cb::logtags::tagUserData(connection.getUsername()),
                    cookie.getEventId());
        break;
    case CBSASL_NO_RBAC_PROFILE:
        LOG_WARNING(
                "{}: User [{}] is not defined as a user in Couchbase. "
                "UUID:[{}]",
                connection.getId(),
                cb::logtags::tagUserData(connection.getUsername()),
                cookie.getEventId());
        break;
    }

    if (error == CBSASL_OK) {
        connection.setAuthenticated(true);
        connection.setInternal(context.second);
        audit_auth_success(&connection);
        LOG_INFO("{}: Client {} authenticated as {}",
                 connection.getId(),
                 connection.getPeername(),
                 cb::logtags::tagUserData(connection.getUsername()));

        /* associate the connection with the appropriate bucket */
        std::string username = connection.getUsername();
        auto idx = username.find(";legacy");
        if (idx != username.npos) {
            username.resize(idx);
        }

        if (cb::rbac::mayAccessBucket(connection.getUsername(), username)) {
            associate_bucket(connection, username.c_str());
        } else {
            // the user don't have access to that bucket, move the
            // connection to the "no bucket"
            associate_bucket(connection, "");
        }
    }

    notify_io_complete(static_cast<void*>(&cookie), ENGINE_SUCCESS);
}
