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

#include "cbsasl/cbsasl_internal.h"

#include <stdexcept>

CBSASL_PUBLIC_API
void cbsasl_dispose(cbsasl_conn_t** conn) {
    if (conn != nullptr) {
        delete *conn;
        *conn = nullptr;
    }
}

static const bool use_saslauthd{getenv("CBAUTH_SOCKPATH") != nullptr};

bool cbsasl_use_saslauthd() {
    return use_saslauthd;
}

CBSASL_PUBLIC_API
cb::sasl::Domain cb::sasl::to_domain(const std::string& domain) {
    if (domain == "builtin") {
        return cb::sasl::Domain::Builtin;
    } else if (domain == "saslauthd") {
        return cb::sasl::Domain::Saslauthd;
    }
    throw std::invalid_argument("cb::sasl::to_domain: invalid domain " +
                                domain);
}

CBSASL_PUBLIC_API
std::string cb::sasl::to_string(cb::sasl::Domain& domain) {
    switch (domain) {
    case cb::sasl::Domain::Builtin:
        return "builtin";
    case cb::sasl::Domain::Saslauthd:
        return "saslauthd";
    }
    throw std::invalid_argument("cb::sasl::to_string: invalid domain " +
                                std::to_string(int(domain)));
}
