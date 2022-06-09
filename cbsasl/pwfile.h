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
#include <functional>
#include <string>

/**
 * Searches for a user entry for the specified user.
 *
 * @param user the username to search for
 * @param user updated with the user information if found
 * @return true if user exists, false otherwise
 */
bool find_user(const std::string& username, cb::sasl::pwdb::User& user);

/**
 * (Re)Load the user database (specified by the environment variable
 * CBSASL_PWFILE) from disk and install as the current database.
 *
 * @param usercallback If provided the the callback will get fired for
 *                     every user in the database _after_ the database
 *                     was installed as the current database to be used
 *                     from the rest of the system.
 */
cb::sasl::Error load_user_db(
        std::function<void(const cb::sasl::pwdb::User&)> usercallback = {});

namespace cb::sasl::pwdb {
class PasswordDatabase;
}

/// Utiliy function for unit tests to avoid having to go through a file in
/// order to write tests which touch code which may call find_user
void swap_password_database(
        std::unique_ptr<cb::sasl::pwdb::PasswordDatabase> database);
