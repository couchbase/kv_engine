/*
 *     Copyright 2013-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "pwfile.h"

#include <cbsasl/plain/plain.h>
#include <cbsasl/scram-sha/scram-sha.h>
#include <cbsasl/server.h>
#include <fmt/format.h>
#include <sodium.h>
#include <memory>
#include <stdexcept>
#include <string>

namespace cb::sasl::server {

std::string listmech() {
    return "SCRAM-SHA512 SCRAM-SHA256 SCRAM-SHA1 PLAIN";
}

std::pair<cb::sasl::Error, std::string_view> ServerContext::start(
        const std::string& mech,
        const std::string& available,
        std::string_view input) {
    if (input.empty()) {
        return std::make_pair<cb::sasl::Error, std::string_view>(
                Error::BAD_PARAM, {});
    }

    switch (selectMechanism(mech, available.empty() ? listmech() : available)) {
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
