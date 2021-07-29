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
#include <string>

/**
 * Searches for a user entry for the specified user.
 *
 * @param user the username to search for
 * @param user updated with the user information if found
 * @return true if user exists, false otherwise
 */
bool find_user(const std::string& username, cb::sasl::pwdb::User& user);

cb::sasl::Error load_user_db();
