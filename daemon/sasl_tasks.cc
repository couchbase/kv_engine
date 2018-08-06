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

#include <cbsasl/mechanism.h>
#include <memcached/rbac.h>
#include <utilities/logtags.h>
#include "connection.h"
#include "cookie.h"
#include "mcaudit.h"
#include "memcached.h"

StartSaslAuthTask::StartSaslAuthTask(Cookie& cookie_,
                                     Connection& connection_,
                                     const std::string& mechanism_,
                                     const std::string& challenge_)
    : SaslAuthTask(cookie_, connection_, mechanism_, challenge_) {
    // No extra initialization needed
}

Task::Status StartSaslAuthTask::execute() {
    connection.restartAuthentication();
    auto& server = connection.getSaslConn();

    try {
        if (connection.isSslEnabled()) {
            response = server.start(
                    mechanism, settings.getSslSaslMechanisms(), challenge);
        } else {
            response = server.start(
                    mechanism, settings.getSaslMechanisms(), challenge);
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
    auto& server = connection.getSaslConn();

    try {
        response = server.step(challenge);
    } catch (const std::bad_alloc&) {
        LOG_WARNING("{}: StepSaslAuthTask::execute(): std::bad_alloc",
                    connection.getId());
        response.first = cb::sasl::Error::NO_MEM;
    } catch (const std::exception& exception) {
        // If we generated an error as part of SASL, we should
        // return that back to the client
        if (server.containsUuid()) {
            cookie.setEventId(server.getUuid());
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

SaslAuthTask::SaslAuthTask(Cookie& cookie_,
                           Connection& connection_,
                           const std::string& mechanism_,
                           const std::string& challenge_)
    : cookie(cookie_),
      connection(connection_),
      mechanism(mechanism_),
      challenge(challenge_) {
    // no more init needed
}

void SaslAuthTask::notifyExecutionComplete() {
    connection.setAuthenticated(false);
    std::pair<cb::rbac::PrivilegeContext, bool> context;

    // If CBSASL generated a UUID, we should continue to use that UUID
    if (connection.getSaslConn().containsUuid()) {
        cookie.setEventId(connection.getSaslConn().getUuid());
    }

    if (response.first == cb::sasl::Error::OK) {
        // Authentication successful, but it still has to be defined in
        // our system
        try {
            context = cb::rbac::createInitialContext(connection.getUsername(),
                                                     connection.getDomain());
        } catch (const cb::rbac::NoSuchUserException&) {
            response.first = cb::sasl::Error::NO_RBAC_PROFILE;
        }
    }

    // Perform the appropriate logging for each error code
    switch (response.first) {
    case cb::sasl::Error::OK:
        // Success
        connection.setAuthenticated(true);
        connection.setInternal(context.second);
        audit_auth_success(&connection);
        LOG_INFO("{}: Client {} authenticated as {}",
                 connection.getId(),
                 connection.getPeername(),
                 cb::UserDataView(connection.getUsername()));

        /* associate the connection with the appropriate bucket */
        {
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
        break;
    case cb::sasl::Error::CONTINUE:
        LOG_DEBUG("{}: SASL CONTINUE", connection.getId());
        break;
    case cb::sasl::Error::FAIL:
    case cb::sasl::Error::BAD_PARAM:
    case cb::sasl::Error::NO_MEM:
    case cb::sasl::Error::NO_MECH:
        // Should already have been logged
        break;

    case cb::sasl::Error::NO_USER:
        LOG_WARNING("{}: User [{}] not found. UUID:[{}]",
                    connection.getId(),
                    cb::UserDataView(connection.getUsername()),
                    cookie.getEventId());
        break;

    case cb::sasl::Error::PASSWORD_ERROR:
        LOG_WARNING("{}: Invalid password specified for [{}] UUID:[{}]",
                    connection.getId(),
                    cb::UserDataView(connection.getUsername()),
                    cookie.getEventId());
        break;
    case cb::sasl::Error::NO_RBAC_PROFILE:
        LOG_WARNING(
                "{}: User [{}] is not defined as a user in Couchbase. "
                "UUID:[{}]",
                connection.getId(),
                cb::UserDataView(connection.getUsername()),
                cookie.getEventId());
        break;
    }

    notify_io_complete(static_cast<void*>(&cookie), ENGINE_SUCCESS);
}
