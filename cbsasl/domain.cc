/*
 *     Copyright 2018-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include <cbsasl/domain.h>
#include <fmt/format.h>
#include <stdexcept>

/// We're using domains in (at least) two places in our system: in cbsasl
/// and audit. In the initial version of the audit daemon we allowed for
/// "builtin" to be used, and we didn't have a strict check for what
/// the various components submitted. When support for audit filtering
/// was added the UI wants the user to use "couchbase" for the domain
/// and external for the others. Allow "builtin" and "couchbase" as "aliases"
/// for "local" until all components submit using "local"
cb::sasl::Domain cb::sasl::to_domain(std::string_view domain) {
    using namespace std::string_view_literals;
    if (domain == "local"sv || domain == "builtin"sv ||
        domain == "couchbase"sv) {
        return cb::sasl::Domain::Local;
    } else if (domain == "external") {
        return cb::sasl::Domain::External;
    }
    throw std::invalid_argument(
            fmt::format("cb::sasl::to_domain: invalid domain: {}", domain));
}

std::string to_string(cb::sasl::Domain domain) {
    switch (domain) {
    case cb::sasl::Domain::Local:
        return "local";
    case cb::sasl::Domain::External:
        return "external";
    }
    throw std::invalid_argument("cb::sasl::to_string: invalid domain " +
                                std::to_string(int(domain)));
}
