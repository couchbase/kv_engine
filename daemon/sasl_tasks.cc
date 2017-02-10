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
    error = cbsasl_server_start(connection.getSaslConn(),
                                mechanism.c_str(),
                                challenge.data(),
                                static_cast<unsigned int>(challenge.length()),
                                &response, &response_length);
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
    error = cbsasl_server_step(connection.getSaslConn(), challenge.data(),
                               static_cast<unsigned int>(challenge.length()),
                               &response, &response_length);
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

    if (error == CBSASL_OK) {
        // Authentication successful, but it still has to be defined in
        // our system
        try {
            cb::rbac::createContext(connection.getUsername(), "");
        } catch (const cb::rbac::NoSuchUserException& e) {
            error = CBSASL_NO_RBAC_PROFILE;
            LOG_WARNING(&connection,
                        "%u: User [%s] is not defined as a user in Couchbase",
                        connection.getId(), connection.getUsername());
        }
    }

    if (error == CBSASL_OK) {
        connection.setAuthenticated(true);
        audit_auth_success(&connection);
        LOG_INFO(&connection, "%u: Client %s authenticated as %s",
                 connection.getId(), connection.getPeername().c_str(),
                 connection.getUsername());

        // @todo this should be from the security context of the user
        connection.setInternal(std::string{"_admin"} == connection.getUsername());

        /* associate the connection with the appropriate bucket */
        associate_bucket(&connection, connection.getUsername());
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
