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

#pragma once

#include <mcbp/protocol/status.h>

#include "authn_authz_service_task.h"
#include <cbsasl/server.h>
#include <string>

class Connection;
class Cookie;

namespace cb::sasl::server {
class ServerContext;
} // namespace cb::sasl::server

/**
 * The SaslAuthTask is the abstract base class used during SASL
 * authentication (which is being run by the executor service)
 */
class StartSaslAuthTask : public AuthnAuthzServiceTask {
public:
    StartSaslAuthTask(Cookie& cookie_,
                      cb::sasl::server::ServerContext& serverContext_,
                      std::string mechanism_,
                      std::string challenge_);

    cb::sasl::Error getError() const {
        return error;
    }

    const std::string& getMechanism() const {
        return mechanism;
    }

    const std::string& getChallenge() const {
        return challenge;
    }

    cb::rbac::UserIdent getUser() const {
        return serverContext.getUser();
    }

    void externalResponse(cb::mcbp::Status status,
                          const std::string& payload) override;

    std::string getUsername() const;

protected:
    void successfull_external_auth();
    void unsuccessfull_external_auth(cb::mcbp::Status status,
                                     const std::string& payload);

    Cookie& cookie;
    cb::sasl::server::ServerContext& serverContext;
    std::string mechanism;
    std::string challenge;
    cb::sasl::Error error = cb::sasl::Error::FAIL;
};
