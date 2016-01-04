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

#include <platform/cbassert.h>

#include <cstring>
#include <mutex>
#include <stdio.h>
#include <stdlib.h>
#include <stdexcept>
#include <string>
#include <unordered_map>

static std::mutex uhash_lock;

// Map of username -> password.
typedef std::unordered_map<std::string, std::string> user_hashtable_t;
static user_hashtable_t user_ht;

static void kill_whitey(char *s)
{
    for (size_t i = strlen(s) - 1; i > 0 && isspace(s[i]); i--) {
        s[i] = '\0';
    }
}

static const char *get_isasl_filename(void)
{
    return getenv("ISASL_PWFILE");
}

void free_user_ht(void)
{
    user_ht.clear();
}

static void store_pw(user_hashtable_t& ht,
                     const char *username,
                     const char *password)
{
    ht.emplace(std::make_pair(username, password));
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

cbsasl_error_t load_user_db(void)
{
    FILE *sfile;
    char up[128];
    const char *filename = get_isasl_filename();


    if (!filename) {
        return CBSASL_OK;
    }

    sfile = fopen(filename, "r");
    if (!sfile) {
        return CBSASL_FAIL;
    }

    try {
        user_hashtable_t new_ut;

        /* File has lines that are newline terminated. */
        /* File may have comment lines that must being with '#'. */
        /* Lines should look like... */
        /*   <NAME><whitespace><PASSWORD><whitespace><CONFIG><optional_whitespace> */
        /* */
        while (fgets(up, sizeof(up), sfile)) {
            if (up[0] != '#') {
                char *uname = up, *p = up, *cfg = NULL;
                kill_whitey(up);
                while (*p && !isspace(p[0])) {
                    p++;
                }
                /* If p is pointing at a NUL, there's nothing after the username. */
                if (p[0] != '\0') {
                    p[0] = '\0';
                    p++;
                }
                /* p now points to the first character after the (now) */
                /* null-terminated username. */
                while (*p && isspace(*p)) {
                    p++;
                }
                /* p now points to the first non-whitespace character */
                /* after the above */
                cfg = p;
                if (cfg[0] != '\0') {
                    /* move cfg past the password */
                    while (*cfg && !isspace(cfg[0])) {
                        cfg++;
                    }
                    if (cfg[0] != '\0') {
                        cfg[0] = '\0';
                        cfg++;
                        /* Skip whitespace */
                        while (*cfg && isspace(cfg[0])) {
                            cfg++;
                        }
                    }
                }
                /* Note: cfg currently unused and hence not stored in hash. */
                store_pw(new_ut, uname, p);
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
