/*
 *     Copyright 2016 Couchbase, Inc.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */
#pragma once

#include "cbsasl/cbsasl.h"
#include "user.h"
#include <string>

/**
 * Searches for a password entry for the specified user.
 *
 * @param user the username to search for
 * @param password updated with the users password if found
 * @return true if user exists, false otherwise
 */
bool find_pw(const std::string& user, std::string& password);

/**
 * Searches for a user entry for the specified user.
 *
 * @param user the username to search for
 * @param user updated with the user information if found
 * @return true if user exists, false otherwise
 */
bool find_user(const std::string& username, Couchbase::User &user);

cbsasl_error_t load_user_db(void);

void free_user_ht(void);
