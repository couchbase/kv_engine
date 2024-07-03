/*
 *     Copyright 2013-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "oauthbearer/oauthbearer.h"
#include "plain/plain.h"
#include "pwfile.h"
#include "scram-sha/scram-sha.h"
#include <cbsasl/server.h>
#include <fmt/format.h>
#include <sodium.h>
#include <atomic>
#include <memory>
#include <stdexcept>
#include <string>

namespace cb::sasl::server {
static std::atomic_bool using_external_auth_service{false};

void set_using_external_auth_service(bool value) {
    using_external_auth_service = value;
}

std::string listmech(bool tls) {
    if (tls) {
        return "SCRAM-SHA512 SCRAM-SHA256 SCRAM-SHA1 PLAIN OAUTHBEARER";
    }
    return "SCRAM-SHA512 SCRAM-SHA256 SCRAM-SHA1 PLAIN";
}

pwdb::User ServerContext::lookupUser(const std::string& username) {
    if (lookup_user_function) {
        return lookup_user_function(username);
    }

    return ::find_user(username);
}

bool ServerContext::bypassAuthForUnknownUsers() const {
    return (using_external_auth_service && !lookup_user_function);
}

std::pair<Error, std::string> ServerContext::start(
        const std::string_view mech,
        const std::string_view available,
        std::string_view input) {
    if (input.empty()) {
        return {Error::BAD_PARAM, {}};
    }

    switch (selectMechanism(mech, available.empty() ? listmech() : available)) {
    case Mechanism::OAUTHBEARER:
        backend =
                std::make_unique<mechanism::oauthbearer::ServerBackend>(*this);
        break;
    case Mechanism::SCRAM_SHA512:
        backend =
                std::make_unique<mechanism::scram::Sha512ServerBackend>(*this);
        break;
    case Mechanism::SCRAM_SHA256:
        backend =
                std::make_unique<mechanism::scram::Sha256ServerBackend>(*this);
        break;
    case Mechanism::SCRAM_SHA1:
        backend = std::make_unique<mechanism::scram::Sha1ServerBackend>(*this);
        break;
    case Mechanism::PLAIN:
        backend = std::make_unique<mechanism::plain::ServerBackend>(*this);
        break;
    }

    return backend->start(input);
}

std::pair<Error, std::string> ServerContext::step(std::string_view input) {
    if (lookup_user_function) {
        lookup_user_function = [](auto&) -> pwdb::User {
            throw std::runtime_error(
                    "ServerContext::step() must not try to lookup user");
        };
    }
    return backend->step(input);
}

Error reload_password_database(
        const std::function<void(const pwdb::User&)>& usercallback) {
    return load_user_db(usercallback);
}

void initialize() {
    if (sodium_init() == -1) {
        throw std::runtime_error(
                "cb::sasl::server::initialize: sodium_init failed");
    }

    const auto ret = load_user_db();
    if (ret != Error::OK) {
        throw std::runtime_error(fmt::format(
                "cb::sasl::server::initialize: Failed to load database: {}",
                ret));
    }
}

void shutdown() {
}

} // namespace cb::sasl::server
