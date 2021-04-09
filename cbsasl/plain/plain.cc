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

#include <cbsasl/logging.h>
#include <platform/dirutils.h>
#include <cstring>

namespace cb::sasl::mechanism::plain {

/**
 * ns_server creates a legacy bucket user as part of the upgrade
 * process which is used by the XDCR clients when they connect
 * to they system. These clients _always_ connect by using PLAIN
 * authentication so we should look up and try those users first.
 * If it exists and we have a matching password we're good to go,
 * otherwise we'll have to try the "normal" user.
 */
bool ServerBackend::try_legacy_user(const std::string& password) {
    const std::string lecacy_username{username + ";legacy"};
    cb::sasl::pwdb::User user;
    if (!find_user(lecacy_username, user)) {
        return false;
    }

    if (cb::sasl::plain::check_password(&context, user, password) ==
        Error::OK) {
        username.assign(lecacy_username);
        return true;
    }

    return false;
}

std::pair<Error, std::string_view> ServerBackend::start(
        std::string_view input) {
    if (input.empty()) {
        return std::make_pair<Error, std::string_view>(Error::BAD_PARAM, {});
    }

    // Skip everything up to the first \0
    size_t inputpos = 0;
    while (inputpos < input.size() && input[inputpos] != '\0') {
        inputpos++;
    }
    inputpos++;

    if (inputpos >= input.size()) {
        return std::make_pair<Error, std::string_view>(Error::BAD_PARAM, {});
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

    if (try_legacy_user(userpw)) {
        return std::make_pair<Error, std::string_view>(Error::OK, {});
    }

    cb::sasl::pwdb::User user;
    if (!find_user(username, user)) {
        return std::pair<Error, std::string_view>{Error::NO_USER, {}};
    }

    return std::make_pair<Error, std::string_view>(
            cb::sasl::plain::check_password(&context, user, userpw), {});
}

std::pair<Error, std::string_view> ClientBackend::start() {
    auto usernm = usernameCallback();
    auto passwd = passwordCallback();

    buffer.push_back(0);
    std::copy(usernm.begin(), usernm.end(), std::back_inserter(buffer));
    buffer.push_back(0);
    std::copy(passwd.begin(), passwd.end(), std::back_inserter(buffer));

    return std::make_pair<Error, std::string_view>(
            Error::OK, {buffer.data(), buffer.size()});
}

Error authenticate(const std::string& username, const std::string& passwd) {
    cb::sasl::pwdb::User user;
    if (!find_user(username, user)) {
        return Error::NO_USER;
    }

    return cb::sasl::plain::check_password(nullptr, user, passwd);
}

} // namespace cb::sasl::mechanism::plain
