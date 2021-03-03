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
#pragma once

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
class SaslAuthTask : public AuthnAuthzServiceTask {
public:
    SaslAuthTask() = delete;

    SaslAuthTask(const SaslAuthTask&) = delete;

    SaslAuthTask(Cookie& cookie_,
                 cb::sasl::server::ServerContext& serverContext_,
                 std::string mechanism_,
                 std::string challenge_);

    void notifyExecutionComplete() override;

    cb::sasl::Error getError() const {
        return response.first;
    }

    std::string_view getResponse() const {
        return response.second;
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

protected:
    Cookie& cookie;
    cb::sasl::server::ServerContext& serverContext;
    std::string mechanism;
    std::string challenge;
    std::pair<cb::sasl::Error, std::string_view> response{cb::sasl::Error::FAIL,
                                                          {}};
};
