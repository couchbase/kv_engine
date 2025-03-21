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

#include "authn_authz_service_task.h"
#include <cbsasl/server.h>
#include <mcbp/protocol/status.h>
#include <string>

class Connection;
class Cookie;

/**
 * The SaslAuthTask is the abstract base class used during SASL
 * authentication (which is being run by the executor service)
 */
class SaslAuthTask : public AuthnAuthzServiceTask {
public:
    SaslAuthTask(Cookie& cookie,
                 cb::sasl::server::ServerContext& serverContext,
                 std::string mechanism,
                 std::string challenge);

    cb::sasl::Error getError() const {
        return error;
    }

    std::string_view getMechanism() const {
        return mechanism;
    }

    std::string_view getChallenge() const {
        return challenge;
    }

    std::string_view getContext() const {
        return context;
    }

    cb::rbac::UserIdent getUser() const {
        return serverContext.getUser();
    }

    void externalResponse(cb::mcbp::Status status,
                          std::string_view payload) override;

    void logIfSlowResponse() const;

    std::string getUsername() const;

    nlohmann::json getPeer() const;

    void updateExternalAuthContext() {
        serverContext.setExternalServerContext(std::move(context));
    }

    auto getTokenMetadata() const {
        return tokenMetadata;
    }

    auto getAdditionalAuditInformation() const {
        return additionalAuditInformation;
    }

protected:
    void successfull_external_auth(const nlohmann::json& json);
    void unsuccessfull_external_auth(cb::mcbp::Status status,
                                     const nlohmann::json& json);

    // @todo I might need folly::synchronized on the ones I update in
    //       the external one?

    Cookie& cookie;
    cb::sasl::server::ServerContext& serverContext;
    std::string mechanism;
    std::string challenge;
    std::string context;
    std::optional<nlohmann::json> tokenMetadata;
    std::optional<nlohmann::json> additionalAuditInformation;
    cb::sasl::Error error = cb::sasl::Error::FAIL;
};
