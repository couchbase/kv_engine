/*
 *     Copyright 2024-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "oauthbearer.h"

#include <cbsasl/username_util.h>
#include <fmt/format.h>
#include <platform/split_string.h>

namespace cb::sasl::mechanism::oauthbearer {

std::pair<Error, std::string> ServerBackend::start(std::string_view input) {
    // Currently a super scaled down version which only works with our
    // own impl ;)
    auto fields = cb::string::split(input, 0x01);
    if (fields.size() < 2) {
        return {Error::BAD_PARAM, {}};
    }

    auto gs2 = fields.front();
    if (!gs2.starts_with("n,a=")) {
        return {Error::BAD_PARAM, {}};
    }
    username = std::string(gs2.substr(4));
    if (username.empty()) {
        return {Error::BAD_PARAM, {}};
    }
    if (username.back() == ',') {
        username.pop_back();
    }
    username = username::decode(username);
    auto token = fields[1];
    if (!token.starts_with("auth=Bearer ")) {
        return {Error::BAD_PARAM, {}};
    }
    token.remove_prefix(12);
    return {context.validateUserToken(username, token), {}};
}

std::pair<Error, std::string> ClientBackend::start() {
    std::string header = fmt::format("n,a={},", usernameCallback());
    header.push_back(0x01);
    header.append(fmt::format("auth=Bearer {}", passwordCallback()));
    header.push_back(0x01);
    header.push_back(0x01);
    return {Error::OK, std::move(header)};
}

} // namespace cb::sasl::mechanism::oauthbearer