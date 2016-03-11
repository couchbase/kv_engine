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

StartSaslAuthTask::StartSaslAuthTask(Connection& connection_,
                                     const std::string& mechanism_,
                                     const std::string& challenge_)
    : SaslAuthTask(connection_, mechanism_, challenge_) {
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

StepSaslAuthTask::StepSaslAuthTask(Connection& connection_,
                                   const std::string& mechanism_,
                                   const std::string& challenge_)
    : SaslAuthTask(connection_, mechanism_, challenge_) {
    // No extra initialization needed
}

bool StepSaslAuthTask::execute() {
    error = cbsasl_server_step(connection.getSaslConn(), challenge.data(),
                               static_cast<unsigned int>(challenge.length()),
                               &response, &response_length);
    return true;
}


SaslAuthTask::SaslAuthTask(Connection& connection_,
                           const std::string& mechanism_,
                           const std::string& challenge_)
    : connection(connection_),
      mechanism(mechanism_),
      challenge(challenge_),
      error(CBSASL_FAIL),
      response(nullptr),
      response_length(0) {
    // no more init needed
}

void SaslAuthTask::notifyExecutionComplete() {
    if (error == CBSASL_OK) {
        connection.setAuthenticated(true);

        audit_auth_success(&connection);
        LOG_INFO(&connection, "%u: Client %s authenticated as %s",
                 connection.getId(), connection.getPeername().c_str(),
                 connection.getUsername());

        /*
         * We've successfully changed our user identity.
         * Update the authentication context
         */
        connection.setAuthContext(auth_create(connection.getUsername(),
                                              connection.getPeername().c_str(),
                                              connection.getSockname().c_str()));

        if (settings.disable_admin) {
            /* "everyone is admins" */
            connection.setAdmin(true);
        } else if (settings.admin != NULL) {
            if (strcmp(settings.admin, connection.getUsername()) == 0) {
                connection.setAdmin(true);
            }
        }

        /* associate the connection with the appropriate bucket */
        associate_bucket(&connection, connection.getUsername());
    } else {
        connection.setAuthenticated(false);
    }

    // notifyExecutionComplete is called from the executor while holding
    // the mutex lock, but we're calling notify_io_complete which in turn
    // tries to lock the threads lock. We held that lock when we scheduled
    // the task, so thread sanitizer will complain about potential
    // deadlock since we're now locking in the oposite order.
    // Given that we know that the executor is _blocked_ waiting for
    // this call to complete (and no one else should touch this object
    // while waiting for the call) lets just unlock and lock again..
    getMutex().unlock();
    if (connection.getProtocol() == Protocol::Memcached) {
        notify_io_complete(reinterpret_cast<McbpConnection&>(connection).getCookie(), ENGINE_SUCCESS);
    } else {
        throw std::runtime_error("Not implemented for Greenstack");
    }
    getMutex().lock();
}
