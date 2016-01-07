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
#include "cbsasl_internal.h"
#include "user.h"

#include <cstring>
#include <iterator>
#include <mutex>
#include <sstream>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <vector>
#include <platform/strerror.h>
#include <platform/timeutils.h>

typedef std::unordered_map<std::string, Couchbase::User> user_hashtable_t;

// The mutex protecting access to the user database
static std::mutex uhash_lock;

// The in-memory user database (Map of username -> user objects).
static user_hashtable_t user_ht;

void free_user_ht(void) {
    user_ht.clear();
}

bool find_pw(const std::string& user, std::string& password) {
    std::lock_guard<std::mutex> guard(uhash_lock);
    auto it = user_ht.find(user);
    if (it != user_ht.end()) {
        password = it->second.getPlaintextPassword();
        return true;
    } else {
        return false;
    }
}

cbsasl_error_t load_user_db(void) {
    const char* filename = getenv("ISASL_PWFILE");

    if (!filename) {
        cbsasl_log(nullptr, cbsasl_loglevel_t::Debug,
                   "No password file specified");
        return CBSASL_OK;
    }

    FILE* sfile = fopen(filename, "r");
    if (sfile == nullptr) {
        std::string logmessage(
            "Failed to open [" + std::string(filename) + "]: " + cb_strerror());
        cbsasl_log(nullptr, cbsasl_loglevel_t::Error, logmessage);
        return CBSASL_FAIL;
    }

    auto start = gethrtime();

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

                if (cbsasl_get_loglevel(nullptr) ==
                    cbsasl_loglevel_t::Password) {
                    std::string logmessage(
                        "Adding user " + tokens[0] + " [" + passwd + "]");
                    cbsasl_log(nullptr, cbsasl_loglevel_t::Password,
                               logmessage);
                } else {
                    std::string logmessage("Adding user " + tokens[0]);
                    cbsasl_log(nullptr, cbsasl_loglevel_t::Debug, logmessage);
                }
                new_ut.emplace(tokens[0], Couchbase::User(tokens[0], passwd));
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

    std::string logmessage("Loading [" + std::string(filename) + "] took " +
                           Couchbase::hrtime2text(gethrtime() - start));
    cbsasl_log(nullptr, cbsasl_loglevel_t::Debug, logmessage);

    return CBSASL_OK;
}
