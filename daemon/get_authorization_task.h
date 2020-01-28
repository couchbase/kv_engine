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
#pragma once

#include "authn_authz_service_task.h"
#include <cbsasl/error.h>
#include <include/memcached/rbac/privilege_database.h>
#include <string>

class Cookie;

/**
 * The GetAuthorizationTask is used to request the external auth provider
 * to fetch the authorization data for the provided user.
 */
class GetAuthorizationTask : public AuthnAuthzServiceTask {
public:
    explicit GetAuthorizationTask(Cookie& cookie,
                                  const cb::rbac::UserIdent& user)
        : cookie(cookie), user(user) {
    }

    Status execute() override;
    void externalResponse(cb::mcbp::Status status,
                          const std::string& payload) override;
    void notifyExecutionComplete() override;

    std::string getUsername() const {
        return user.name;
    }

    cb::sasl::Error getStatus() const {
        return status;
    }

protected:
    Cookie& cookie;
    const cb::rbac::UserIdent& user;
    bool requestSent = false;
    cb::sasl::Error status{cb::sasl::Error::FAIL};
};
