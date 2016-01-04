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
#include <string>

/**
 * Searches for a password entry for the specified user. On success, returns
 * the user's password. On failure
 * returns NULL.
 */
bool find_pw(const std::string& user, std::string& password);

cbsasl_error_t load_user_db(void);

void free_user_ht(void);
