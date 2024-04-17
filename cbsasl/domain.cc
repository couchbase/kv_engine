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
#include <nlohmann/json.hpp>
#include <stdexcept>

/// We're using domains in (at least) two places in our system: in cbsasl
/// and audit. In the initial version of the audit daemon we allowed for
/// "builtin" to be used, and we didn't have a strict check for what
/// the various components submitted. When support for audit filtering
/// was added the UI wants the user to use "couchbase" for the domain
/// and external for the others. Allow "builtin" and "couchbase" as "aliases"
/// for "local" until all components submit using "local"
cb::sasl::Domain cb::sasl::to_domain(const std::string_view domain) {
    using namespace std::string_view_literals;
    if (domain == "local"sv || domain == "builtin"sv ||
        domain == "couchbase"sv) {
        return Domain::Local;
    }
    if (domain == "external") {
        return Domain::External;
    }
    if (domain == "unknown") {
        return Domain::Unknown;
    }
    throw std::invalid_argument(
            fmt::format("cb::sasl::to_domain: invalid domain: {}", domain));
}

std::string cb::sasl::format_as(const Domain domain) {
    switch (domain) {
    case Domain::Local:
        return "local";
    case Domain::External:
        return "external";
    case Domain::Unknown:
        return "unknown";
    }
    throw std::invalid_argument(fmt::format("format_as(): invalid domain: {}",
                                            static_cast<int>(domain)));
}

void cb::sasl::to_json(nlohmann::json& json, const Domain domain) {
    json = format_as(domain);
}