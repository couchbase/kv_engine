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

#include <cbsasl/client.h>
#include <cbsasl/server.h>
#include <stdexcept>
#include <vector>

namespace cb::sasl::mechanism::plain {

class ServerBackend : public server::MechanismBackend {
public:
    explicit ServerBackend(server::ServerContext& ctx)
        : MechanismBackend(ctx){};

    std::pair<Error, std::string_view> start(std::string_view input) override;
    std::pair<Error, std::string_view> step(std::string_view input) override {
        throw std::logic_error(
                "cb::sasl::mechanism::plain::ServerBackend::step(): Plain auth "
                "should not call step");
    }
    std::string getName() const override {
        return "PLAIN";
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
        return "PLAIN";
    }

    std::pair<Error, std::string_view> start() override;

    std::pair<Error, std::string_view> step(std::string_view input) override {
        throw std::logic_error(
                "cb::sasl::mechanism::plain::ClientBackend::step(): Plain auth "
                "should not call step");
    }

private:
    /**
     * Where to store the encoded string:
     * "\0username\0password"
     */
    std::vector<char> buffer;
};

} // namespace cb::sasl::mechanism::plain
