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
    if (input.empty()) {
        return {Error::BAD_PARAM, {}};
    }

    // Skip everything up to the first \0
    size_t inputpos = 0;
    while (inputpos < input.size() && input[inputpos] != '\0') {
        inputpos++;
    }
    inputpos++;

    if (inputpos >= input.size()) {
        return {Error::BAD_PARAM, {}};
    }

    size_t pwlen = 0;
    const char* username = input.data() + inputpos;
    const char* password = nullptr;
    while (inputpos < input.size() && input[inputpos] != '\0') {
        inputpos++;
    }
    inputpos++;

    if (inputpos > input.size()) {
        return std::make_pair<Error, std::string_view>(Error::BAD_PARAM, {});
    } else if (inputpos != input.size()) {
        password = input.data() + inputpos;
        while (inputpos < input.size() && input[inputpos] != '\0') {
            inputpos++;
            pwlen++;
        }
    }

    this->username.assign(username);
    const std::string userpw(password, pwlen);

    cb::sasl::pwdb::User user;
    if (!find_user(username, user)) {
        return {Error::NO_USER, {}};
    }

    return {cb::sasl::plain::check_password(user, userpw), {}};
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
    cb::sasl::pwdb::User user;
    if (!find_user(username, user)) {
        return Error::NO_USER;
    }

    return cb::sasl::plain::check_password(user, passwd);
}

} // namespace cb::sasl::mechanism::plain
