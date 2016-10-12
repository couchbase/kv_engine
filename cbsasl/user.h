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

#include <cbsasl/cbcrypto.h>
#include <cstdint>
#include <string>
#include <vector>
#include <map>
#include <cJSON_utils.h>
#include "cbsasl_internal.h"

namespace Couchbase {
    class UserFactory;

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
          * Create an empty user object
          *
          * A dummy user object is used by SCRAM sasl authentication
          * so that the "attacking" client would need to perform the
          * full authentication step in order to authenticate towards
          * the server instead of returning "no such user" if we failed
          * to look up the user (to respond with the SALT and iteration
          * count).
          *
          * @param unm the username for the object (potentially empty)
          * @param dmy should we create a dummy object or not
          */
        User(const std::string& unm = "", const bool dmy = true)
            : username(unm),
              dummy(dmy) {

        }

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
        friend class UserFactory;

        void generateSecrets(const Mechanism& mech,
                             const std::string& passwd);

        std::map<Mechanism, PasswordMetaData> password;

        std::string username;

        bool dummy;
    };

    /**
     * The UserFactory class is used to generate a User Object from a
     * username and plain text password.
     */
    class UserFactory {
    public:

        /**
         * Construct a new user object
         *
         * @param name username
         * @param pw password
         *
         * @return a newly created dummy object
         */
        static User create(const std::string& name, const std::string& pw);

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
         *            "plain" : "base64 encoded hex version of sha1 hash
         *                       of plain text password"
         *       }
         *
         * @param obj the object containing the JSON description
         * @throws std::runtime_error if there is a syntax error
         */
        static User create(const cJSON* obj);

        /**
         * Construct a dummy user object that may be used in authentication
         *
         * @param name username
         * @param mech generate just the password for this mechanism
         *
         * @return a newly created dummy object
         */
        static User createDummy(const std::string& name,
                                const Mechanism& mech);

        /**
         * Set the default iteration count to use (may be overridden by
         * the cbsasl property function.
         *
         * @param count the new iteration count to use
         */
        static void setDefaultHmacIterationCount(int count);
    };
}
