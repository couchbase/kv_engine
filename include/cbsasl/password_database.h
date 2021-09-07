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

    virtual ~PasswordDatabase() = default;

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
    User find(const std::string& username) const;

    /// Iterate over all of the users in the database
    void iterate(std::function<void(const cb::sasl::pwdb::User&)> usercallback)
            const;

    /**
     * Create a JSON representation of the password database
     */
    nlohmann::json to_json() const;

    /**
     * Create a textual representation (in JSON) of the password database
     */
    std::string to_string() const;

    /**
     * Read the password file from the specified filename.
     *
     * @param filename the name of the file to read
     * @return the content of the file
     * @throws std::exception if an error occurs while reading
     */
    static std::string read_password_file(const std::string& filename);

    /**
     * Write the password data to the specified filename.
     *
     * @param filename the name of the file to write
     * @param content the data to write
     * @throws std::exception if an error occurs while writing the data
     */
    static void write_password_file(const std::string& filename,
                                    const std::string& content);

protected:
    /**
     * The actual user database
     */
    std::unordered_map<std::string, User> db;
};

/// When writing unit tests one may want to add/remove/modify users
class MutablePasswordDatabase : public PasswordDatabase {
public:
    MutablePasswordDatabase() : PasswordDatabase() {
    }
    explicit MutablePasswordDatabase(const nlohmann::json& content);
    void upsert(User user);
    void remove(const std::string& username);
};
} // namespace cb::sasl::pwdb
