/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
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
#include "pwconv.h"
#include "user.h"

#include <cJSON_utils.h>
#include <iterator>
#include <platform/strerror.h>
#include <sstream>
#include <vector>


void cbsasl_pwconv(const std::string& ifile, const std::string& ofile) {
    FILE* sfile = fopen(ifile.c_str(), "r");
    if (sfile == nullptr) {
        throw std::runtime_error(
            "Failed to open [" + ifile + "]: " + cb_strerror());
    }

    unique_cJSON_ptr root(cJSON_CreateObject());
    if (root.get() == nullptr) {
        throw std::bad_alloc();
    }
    auto* users = cJSON_CreateArray();
    if (users == nullptr) {
        throw std::bad_alloc();
    }
    cJSON_AddItemToObject(root.get(), "users", users);

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

            std::istringstream iss(up);
            vector<std::string> tokens{istream_iterator<std::string>{iss},
                                       istream_iterator<std::string>{}};

            if (tokens.empty()) {
                // empty line
                continue;
            }
            std::string passwd;
            if (tokens.size() > 1) {
                passwd = tokens[1];
            }

            Couchbase::User u(Couchbase::User(tokens[0], passwd));
            cJSON_AddItemToArray(users, u.to_json().release());
        }
    }

    fclose(sfile);

    FILE* of = fopen(ofile.c_str(), "w");
    if (of == nullptr) {
        throw std::runtime_error(
            "Failed to open [" + ofile + "]: " + cb_strerror());
    }

    auto text = to_string(root, true);
    auto nw = fprintf(of, "%s\n", text.c_str());
    fclose(of);

    if (nw < int(text.size())) {
        std::string errmsg("Failed to write [" + ofile + "]: " + cb_strerror());
        std::remove(ofile.c_str());
        throw std::runtime_error(errmsg);
    }
}
