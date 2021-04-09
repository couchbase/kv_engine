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

#include <stdexcept>

cb::sasl::Domain cb::sasl::to_domain(const std::string& domain) {
    if (domain == "local") {
        return cb::sasl::Domain::Local;
    } else if (domain == "external") {
        return cb::sasl::Domain::External;
    }
    throw std::invalid_argument("cb::sasl::to_domain: invalid domain " +
                                domain);
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
