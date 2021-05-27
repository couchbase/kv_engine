/*
 *     Copyright 2020-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
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
    GetAuthorizationTask(Cookie& cookie, const cb::rbac::UserIdent& user)
        : cookie(cookie), user(user) {
    }

    void externalResponse(cb::mcbp::Status status,
                          const std::string& payload) override;

    std::string getUsername() const {
        return user.name;
    }

    cb::sasl::Error getStatus() const {
        return status;
    }

protected:
    Cookie& cookie;
    const cb::rbac::UserIdent& user;
    cb::sasl::Error status{cb::sasl::Error::FAIL};
};
