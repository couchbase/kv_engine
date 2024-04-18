/*
 *     Copyright 2018-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "plain.h"

#include "cbsasl/pwfile.h"
#include "check_password.h"

namespace cb::sasl::mechanism::plain {

std::pair<Error, std::string_view> ServerBackend::start(
        std::string_view input) {
    // Format: message = [authzid] UTF8NUL authcid UTF8NUL passwd
    //
    // Skip authzid
    auto index = input.find('\0');
    if (index == std::string_view::npos) {
        return {Error::BAD_PARAM, {}};
    }
    input.remove_prefix(index + 1);

    // authcid (our username)
    index = input.find('\0');
    if (index == std::string_view::npos) {
        return {Error::BAD_PARAM, {}};
    }
    username = std::string{input.substr(0, index)};

    // finally password. Some clients may add an extra terminating
    // NUL character
    auto password = input.substr(index + 1);
    if (!password.empty() && password.back() == '\0') {
        password.remove_suffix(1);
    }
    const auto user = context.lookupUser(username);
    if (user.isDummy()) {
        return {Error::NO_USER, {}};
    }

    return {sasl::plain::check_password(user, password), {}};
}

std::pair<Error, std::string_view> ClientBackend::start() {
    auto usernm = usernameCallback();
    auto passwd = passwordCallback();
    if (usernm.empty() || usernm.find('\0') != std::string::npos ||
        passwd.find('\0') != std::string::npos) {
        return {Error::BAD_PARAM, {}};
    }

    buffer.push_back(0);
    buffer.insert(buffer.end(), usernm.begin(), usernm.end());
    buffer.push_back(0);
    buffer.insert(buffer.end(), passwd.begin(), passwd.end());
    return {Error::OK, buffer};
}

Error authenticate(const std::string& username, const std::string& passwd) {
    auto user = find_user(username);
    if (user.isDummy()) {
        return Error::NO_USER;
    }

    return sasl::plain::check_password(user, passwd);
}

} // namespace cb::sasl::mechanism::plain
