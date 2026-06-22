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
#include "parse_gs2_header.h"

#include <cbcrypto/secret.h>
#include <cbsasl/username_util.h>
#include <fmt/format.h>
#include <platform/split_string.h>
#include <platform/string_utilities.h>

namespace cb::sasl::mechanism::oauthbearer {

std::pair<Error, std::string> ServerBackend::start(std::string_view input) {
    // Currently a super scaled down version which only works with our
    // own impl ;)
    auto fields = cb::string::split(input, 0x01);
    if (fields.size() < 2) {
        return {Error::BAD_PARAM, {}};
    }

    try {
        username = parse_gs2_header(fields.front());
    } catch (const std::invalid_argument&) {
        return {Error::BAD_PARAM, {}};
    }

    std::string_view token;
    for (std::size_t index = 1; index < fields.size(); index++) {
        auto field = fields[index];
        if (!field.starts_with("auth=")) {
            continue;
        }
        field.remove_prefix(5);
        auto kv = cb::string::split(field, ' ');
        if (kv.size() != 2) {
            return {Error::BAD_PARAM, {}};
        }
        const auto auth = cb::tolower(std::string{kv.front()});
        if (auth != "bearer") {
            return {Error::BAD_PARAM, {}};
        }
        token = kv.back();
    }

    if (token.empty()) {
        return {Error::BAD_PARAM, {}};
    }

    return {context.validateUserToken(username, token), {}};
}

std::pair<Error, std::string> ClientBackend::start() {
    cb::crypto::SecretString username_holder(usernameCallback());
    cb::crypto::SecretString password_holder(passwordCallback());
    std::string_view usernm = username_holder;
    std::string_view passwd = password_holder;

    auto header = fmt::format(
            "n,{},",
            usernm.empty() ? ""
                           : fmt::format("a={}", username::encode(usernm)));
    header.push_back(0x01);
    header.append(fmt::format("auth=Bearer {}", passwd));
    header.push_back(0x01);
    header.push_back(0x01);
    return {Error::OK, std::move(header)};
}

} // namespace cb::sasl::mechanism::oauthbearer
