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

#include <array>
#include <cJSON.h>
#include <cbsasl/cbcrypto.h>
#include <cstdint>
#include <string>
#include <vector>
#include <map>
#include <cJSON_utils.h>
#include "cbsasl_internal.h"

namespace Couchbase {
    class User {
    public:
        /**
         * To allow multiple authentication schemes we need to store
         * some metadata for each of the different scheme.
         */
        class PasswordMetaData {
        public:
            /**
             * Create a new instance of the PasswordMetaData
             */
            PasswordMetaData()
                : iteration_count(0) {

            };

            /**
             * Create a new instance of the PasswordMetaData with
             * the specified attributes
             *
             * @param h the password
             * @param s the salt used (Base64 encoded)
             * @param i iteration count
             */
            PasswordMetaData(const std::string& h,
                             const std::string& s = "",
                             int i = 0)
                : salt(s),
                  password(h),
                  iteration_count(i) {

            }

            /**
             * Create a new instance of the PasswordMetaData and
             * initialize it from the specified JSON. The JSON *MUST*
             * be of the following syntax:
             *
             *     {
             *         "h" : "base64 encoded password",
             *         "s" : "base64 encoded salt",
             *         "i" : "iteration count"
             *     }
             *
             * @param obj pointer to the cJSON structure
             */
            PasswordMetaData(cJSON* obj);

            /**
             * This is a helper function used from the unit tests
             * to generate a JSON representation of a user
             */
            cJSON* to_json() const;

            const std::string& getSalt() const {
                return salt;
            }

            const std::string& getPassword() const {
                return password;
            }

            int getIterationCount() const {
                return iteration_count;
            }

        private:
            // Base 64 encoded version of the salt
            std::string salt;

            // The actual password used
            std::string password;

            // The iteration count used for generating the password
            int iteration_count;
        };

        /**
         * Create a dummy user entry.
         *
         * A dummy user object is used by SCRAM sasl authentication
         * so that the "attacking" client would need to perform the
         * full authentication step in order to authenticate towards
         * the server instead of returning "no such user" if we failed
         * to look up the user (to respond with the SALT and iteration
         * count).
         */
        User() : dummy(true) {

        }

        /**
         * Create a new User object with the specified username / password
         * combination. In addition we'll generate a new Salt and a
         * salted SHA1 hashed password.
         *
         * @param unm the username to use
         * @param passwd the password to use
         */
        User(const std::string& unm, const std::string& passwd);

        /**
         * Create a user entry and initialize it from the supplied
         * JSON structure:
         *
         *       {
         *            "n" : "username",
         *            "sha512" : {
         *                "h" : "base64 encoded sha512 hash of the password",
         *                "s" : "base64 encoded salt",
         *                "i" : iteration-count
         *            },
         *            "sha256" : {
         *                "h" : "base64 encoded sha256 hash of the password",
         *                "s" : "base64 encoded salt",
         *                "i" : iteration-count
         *            },
         *            "sha1" : {
         *                "h" : "base64 encoded sha1 hash of the password",
         *                "s" : "base64 encoded salt",
         *                "i" : iteration-count
         *            },
         *            "plain" : "base64 encoded plain text password"
         *       }
         *
         * @param obj the object containing the JSON description
         * @throws std::runtime_error if there is a syntax error
         */
        User(cJSON* obj);

        /**
         * Generate the secrets for a dummy object.
         *
         * We don't want to generate the secrets in the default constructor
         * because it may be replaced with the real secrets if the user
         * exists in the user database (generating secrets is quite
         * expensice CPU wise)
         *
         * @param mech the mechanism trying to use this object (to avoid
         *             running the "CPU expensive" PBKDF2 function for
         *             all of the methods.
         */
        void generateSecrets(const Mechanism& mech);

        /**
         * Get the username for this entry
         */
        const std::string& getUsername() const {
            return username;
        }

        /**
          * Is this a dummy object or not?
          */
        bool isDummy() const {
            return dummy;
        }

        /**
         * Get the password metadata used for the requested mechanism
         *
         * @param mech the mechanism to retrieve the metadata for
         * @return the passwod metadata
         * @throws std::illegal_arguement if the mechanism isn't supported
         */
        const PasswordMetaData& getPassword(const Mechanism& mech) const;

        /**
         * Generate a JSON encoding of this object (it is primarily used
         * from the test suite to generate a user database)
         */
        unique_cJSON_ptr to_json() const;

        /**
         * Generate a textual representation of the user object (in JSON)
         */
        std::string to_string() const;

    protected:
        void generateSecrets(const Mechanism& mech,
                             const std::string& passwd);

        std::map<Mechanism, PasswordMetaData> password;

        std::string username;

        bool dummy;
    };
}
