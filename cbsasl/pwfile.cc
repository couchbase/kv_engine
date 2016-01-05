/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2015 Couchbase, Inc.
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
#include "pwfile.h"

#include <cstring>
#include <iterator>
#include <mutex>
#include <sstream>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <vector>

// Map of username -> password.
typedef std::unordered_map<std::string, std::string> user_hashtable_t;

// The mutex protecting access to the user/password map
static std::mutex uhash_lock;

// The map containing the user/password mappings
static user_hashtable_t user_ht;

void free_user_ht(void) {
    user_ht.clear();
}

bool find_pw(const std::string& user, std::string& password) {
    std::lock_guard<std::mutex> guard(uhash_lock);
    auto it = user_ht.find(user);
    if (it != user_ht.end()) {
        password = it->second;
        return true;
    } else {
        return false;
    }
}

cbsasl_error_t load_user_db(void) {
    const char* filename = getenv("ISASL_PWFILE");

    if (!filename) {
        return CBSASL_OK;
    }

    FILE* sfile = fopen(filename, "r");
    if (sfile == nullptr) {
        return CBSASL_FAIL;
    }

    try {
        user_hashtable_t new_ut;

        /* File has lines that are newline terminated.
         * File may have comment lines that must being with '#'.
         * Lines should look like...
         *   <NAME><whitespace><PASSWORD><whitespace><CONFIG><optional_whitespace>
         */
        char up[128];
        while (fgets(up, sizeof(up), sfile)) {
            if (up[0] != '#') {
                using std::istream_iterator;
                using std::vector;
                using std::string;

                std::istringstream iss(up);
                vector<string> tokens{istream_iterator<string>{iss},
                                      istream_iterator<string>{}};

                if (tokens.empty()) {
                    // empty line
                    continue;
                }
                std::string passwd;
                if (tokens.size() > 1) {
                    passwd = tokens[1];
                }

                new_ut.emplace(std::make_pair(tokens[0], passwd));
            }
        }

        fclose(sfile);
        /* Replace the current configuration with the new one */
        {
            std::lock_guard<std::mutex> guard(uhash_lock);
            free_user_ht();
            user_ht = new_ut;
        }

    } catch (std::bad_alloc&) {
        fclose(sfile);
        return CBSASL_NOMEM;
    }

    return CBSASL_OK;
}
