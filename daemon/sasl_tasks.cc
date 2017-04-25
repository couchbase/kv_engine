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
#include "memcached.h"
#include "mcaudit.h"
#include <memcached/rbac.h>


StartSaslAuthTask::StartSaslAuthTask(Cookie& cookie_,
                                     Connection& connection_,
                                     const std::string& mechanism_,
                                     const std::string& challenge_)
    : SaslAuthTask(cookie_, connection_, mechanism_, challenge_) {
    // No extra initialization needed
}

bool StartSaslAuthTask::execute() {
    connection.restartAuthentication();
    try {
        error = cbsasl_server_start(connection.getSaslConn(),
                                    mechanism.c_str(),
                                    challenge.data(),
                                    static_cast<unsigned int>(challenge.length()),
                                    &response, &response_length);
    } catch (const std::bad_alloc&) {
        LOG_WARNING(nullptr,
                    "%u: StartSaslAuthTask::execute(): std::bad_alloc",
                    connection.getId());
        error = CBSASL_NOMEM;
    } catch (const std::exception& exception) {
        // If we generated an error as part of SASL, we should
        // return that back to the client
        auto& uuid = cb::sasl::get_uuid(connection.getSaslConn());
        if (!uuid.empty()) {
            cookie.setEventId(uuid);
        }
        LOG_WARNING(nullptr,
                    "%u: StartSaslAuthTask::execute(): UUID:[%s] An exception occurred: %s",
                    connection.getId(),
                    cookie.getEventId().c_str(),
                    exception.what());
        cookie.setErrorContext("An exception occurred");
        error = CBSASL_FAIL;
    }

    return true;
}

StepSaslAuthTask::StepSaslAuthTask(Cookie& cookie_,
                                   Connection& connection_,
                                   const std::string& mechanism_,
                                   const std::string& challenge_)
    : SaslAuthTask(cookie_, connection_, mechanism_, challenge_) {
    // No extra initialization needed
}

bool StepSaslAuthTask::execute() {
    try {
        error = cbsasl_server_step(connection.getSaslConn(), challenge.data(),
                                   static_cast<unsigned int>(challenge.length()),
                                   &response, &response_length);
    } catch (const std::bad_alloc&) {
        LOG_WARNING(nullptr,
                    "%u: StepSaslAuthTask::execute(): std::bad_alloc",
                    connection.getId());
        error = CBSASL_NOMEM;
    } catch (const std::exception& exception) {
        // If we generated an error as part of SASL, we should
        // return that back to the client
        auto& uuid = cb::sasl::get_uuid(connection.getSaslConn());
        if (!uuid.empty()) {
            cookie.setEventId(uuid);
        }
        LOG_WARNING(nullptr,
                    "%u: StepSaslAuthTask::execute(): UUID:[%s] An exception occurred: %s",
                    connection.getId(),
                    cookie.getEventId().c_str(),
                    exception.what());
        cookie.setErrorContext("An exception occurred");
        error = CBSASL_FAIL;
    }
    return true;
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
    connection.resetUsernameCache();
    std::pair<cb::rbac::PrivilegeContext, bool> context;

    if (error == CBSASL_OK) {
        // Authentication successful, but it still has to be defined in
        // our system
        try {
            context = cb::rbac::createInitialContext(connection.getUsername(),
                                                     connection.getDomain());
        } catch (const cb::rbac::NoSuchUserException& e) {
            error = CBSASL_NO_RBAC_PROFILE;
            LOG_WARNING(&connection,
                        "%u: User [%s] is not defined as a user in Couchbase",
                        connection.getId(), connection.getUsername());
        }
    }

    if (error == CBSASL_OK) {
        connection.setAuthenticated(true);
        connection.setInternal(context.second);
        audit_auth_success(&connection);
        LOG_INFO(&connection, "%u: Client %s authenticated as %s",
                 connection.getId(), connection.getPeername().c_str(),
                 connection.getUsername());

        /* associate the connection with the appropriate bucket */
        std::string username = connection.getUsername();
        auto idx = username.find(";legacy");
        if (idx != username.npos) {
            username.resize(idx);
        }

        if (cb::rbac::mayAccessBucket(connection.getUsername(), username)) {
            associate_bucket(&connection, username.c_str());
        } else {
            // the user don't have access to that bucket, move the
            // connection to the "no bucket"
            associate_bucket(&connection, "");
        }
    } else {
        connection.setAuthenticated(false);
    }

    if (cookie.command == nullptr) {
        notify_io_complete(
            reinterpret_cast<McbpConnection&>(connection).getCookie(),
            ENGINE_SUCCESS);
    } else {
        throw new std::runtime_error("Not implemented for greenstack");
    }
}
