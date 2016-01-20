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
#pragma once

#include <string>
#include <unordered_map>
#include "user.h"

namespace Couchbase {
    class PasswordDatabase {
    public:
        /**
         * Create an instance of the password database without any
         * entries.
         */
        PasswordDatabase() {
            // Empty
        }

        /**
         * Create an instance of the password database and initialize
         * it with the content of the filename
         *
         * @param content the content for the user database
         * @param file if set to true, the content contains the name
         *             of the file to parse
         * @throws std::runtime_error if an error occurs
         */
        PasswordDatabase(const std::string& content, bool file = true);

        /**
         * Try to locate the user in the password database
         *
         * @param username the username to look up
         * @return a copy of the user object
         */
        Couchbase::User find(const std::string& username) {
            auto it = db.find(username);
            if (it != db.end()) {
                return it->second;
            } else {
                // Return a dummy user (allow the authentication to go
                // through the entire authentication phase but fail with
                // incorrect password ;-)
                return Couchbase::User();
            }
        }

        /**
         * Create a JSON representation of the password database
         */
        unique_cJSON_ptr to_json() const;

        /**
         * Create a textual representation (in JSON) of the password database
         */
        std::string to_string() const;


    private:
        /**
         * The actual user database
         */
        std::unordered_map<std::string, Couchbase::User> db;
    };
}