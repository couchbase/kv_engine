/*
 *     Copyright 2013 Couchbase, Inc.
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


#include "pwfile.h"
#include "util.h"

#include <cbsasl/logging.h>
#include <cbsasl/plain/plain.h>
#include <cbsasl/scram-sha/scram-sha.h>
#include <cbsasl/server.h>

#include <platform/random.h>

#include <memory>
#include <string>

namespace cb {
namespace sasl {
namespace server {

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

std::pair<cb::sasl::Error, cb::const_char_buffer> ServerContext::start(
        const std::string& mech,
        const std::string& available,
        cb::const_char_buffer input) {
    if (input.empty()) {
        return std::make_pair<cb::sasl::Error, cb::const_char_buffer>(
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

} // namespace server
} // namespace sasl
} // namespace cb
