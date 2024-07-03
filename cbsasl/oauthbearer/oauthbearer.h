/*
 *     Copyright 2024-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#pragma once

#include <cbsasl/client.h>
#include <cbsasl/server.h>
#include <stdexcept>

/// Extremely simple prototype of an OAUTHBEARER mechanism as
/// desceribed in https://datatracker.ietf.org/doc/html/rfc7628 and
/// https://datatracker.ietf.org/doc/html/rfc6750
namespace cb::sasl::mechanism::oauthbearer {

class ServerBackend : public server::MechanismBackend {
public:
    explicit ServerBackend(server::ServerContext& ctx) : MechanismBackend(ctx) {
    }

    std::pair<Error, std::string> start(std::string_view input) override;
    std::pair<Error, std::string> step(std::string_view input) override {
        throw std::logic_error(
                "ServerBackend::step(): OAUTHBEARER should not call step");
    }
    std::string getName() const override {
        return "OAUTHBEARER";
    }
};

class ClientBackend : public client::MechanismBackend {
public:
    ClientBackend(client::GetUsernameCallback& user_cb,
                  client::GetPasswordCallback& password_cb,
                  client::ClientContext& ctx)
        : MechanismBackend(user_cb, password_cb, ctx) {
    }

    std::string getName() const override {
        return "OAUTHBEARER";
    }

    std::pair<Error, std::string> start() override;

    std::pair<Error, std::string> step(std::string_view input) override {
        throw std::logic_error(
                "ClientBackend::step(): OAUTHBEARER auth should not call step");
    }
};

} // namespace cb::sasl::mechanism::oauthbearer
