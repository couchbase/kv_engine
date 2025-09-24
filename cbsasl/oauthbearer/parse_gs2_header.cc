/*
 *     Copyright 2025-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "parse_gs2_header.h"

#include "cbsasl/username_util.h"
#include <platform/split_string.h>
#include <stdexcept>

namespace cb::sasl::mechanism::oauthbearer {
std::string parse_gs2_header(std::string_view input) {
    const auto fields = cb::string::split(input, ',');
    if (fields.size() != 2) {
        throw std::invalid_argument("Invalid GS2 header");
    }

    if (fields[0] != "n") {
        throw std::invalid_argument(
                "Only 'n' (no channel binding) is supported");
    }

    std::string authzid;
    if (!fields[1].empty()) {
        if (!fields[1].starts_with("a=")) {
            throw std::invalid_argument("Invalid GS2 header");
        }
        authzid = username::decode(fields[1].substr(2));
        if (authzid.empty()) {
            throw std::invalid_argument("Invalid saslname");
        }
    }

    return authzid;
}
} // namespace cb::sasl::mechanism::oauthbearer
