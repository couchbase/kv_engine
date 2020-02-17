/*
 *     Copyright 2018 Couchbase, Inc.
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

#include <cbsasl/client.h>
#include <cbsasl/server.h>
#include <vector>

namespace cb {
namespace sasl {
namespace mechanism {
namespace plain {

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

protected:
    bool try_legacy_user(const std::string& password);
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

} // namespace plain
} // namespace mechanism
} // namespace sasl
} // namespace cb
