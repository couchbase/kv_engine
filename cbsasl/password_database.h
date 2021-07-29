/*
 *     Copyright 2016-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#pragma once

#include <cbsasl/user.h>
#include <nlohmann/json_fwd.hpp>
#include <functional>
#include <string>
#include <unordered_map>

namespace cb::sasl::pwdb {

class PasswordDatabase {
public:
    /**
     * Create an instance of the password database without any
     * entries.
     */
    PasswordDatabase() = default;

    /**
     * Create an instance of the password database and initialize
     * it with the content of the filename
     *
     * @param content the content for the user database
     * @param file if set to true, the content contains the name
     *             of the file to parse
     * @throws std::runtime_error if an error occurs
     */
    explicit PasswordDatabase(const std::string& content, bool file = true);

    /**
     * Try to locate the user in the password database
     *
     * @param username the username to look up
     * @return a copy of the user object
     */
    User find(const std::string& username) {
        auto it = db.find(username);
        if (it != db.end()) {
            return it->second;
        } else {
            // Return a dummy user (allow the authentication to go
            // through the entire authentication phase but fail with
            // incorrect password ;-)
            return User();
        }
    }

    /// Iterate over all of the users in the database
    void iterate(
            std::function<void(const cb::sasl::pwdb::User&)> usercallback) {
        for (const auto& entry : db) {
            usercallback(entry.second);
        }
    }

    /**
     * Create a JSON representation of the password database
     */
    nlohmann::json to_json() const;

    /**
     * Create a textual representation (in JSON) of the password database
     */
    std::string to_string() const;

private:
    /**
     * The actual user database
     */
    std::unordered_map<std::string, User> db;
};
} // namespace cb::sasl::pwdb
