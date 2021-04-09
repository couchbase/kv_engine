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
#include "util.h"

#include <cbsasl/logging.h>
#include <cbsasl/plain/plain.h>
#include <cbsasl/scram-sha/scram-sha.h>
#include <cbsasl/server.h>

#include <platform/random.h>

#include <memory>
#include <string>

namespace cb::sasl::server {

std::string listmech() {
    std::string ret;

    using namespace cb::crypto;

    if (isSupported(Algorithm::SHA512)) {
        ret.append("SCRAM-SHA512 ");
    }

    if (isSupported(Algorithm::SHA256)) {
        ret.append("SCRAM-SHA256 ");
    }

    if (isSupported(Algorithm::SHA1)) {
        ret.append("SCRAM-SHA1 ");
    }

    ret.append("PLAIN");

    return ret;
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
    case Mechanism::SCRAM_SHA1:;
        backend = std::make_unique<mechanism::scram::Sha1ServerBackend>(*this);
        break;
    case Mechanism::PLAIN:
        backend = std::make_unique<mechanism::plain::ServerBackend>(*this);
        break;
    }

    return backend->start(input);
}

cb::sasl::Error refresh() {
    return load_user_db();
}

void initialize() {
    const auto ret = load_user_db();
    if (ret != Error::OK) {
        throw std::runtime_error(
                "cb::sasl::server::initialize: Failed to load database: " +
                ::to_string(ret));
    }
}

void shutdown() {
}

void set_hmac_iteration_count(int count) {
    pwdb::UserFactory::setDefaultHmacIterationCount(count);
}

void set_scramsha_fallback_salt(const std::string& salt) {
    pwdb::UserFactory::setScramshaFallbackSalt(salt);
}

} // namespace cb::sasl::server
