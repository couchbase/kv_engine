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
#include <cstdint>
#include <string>
#include "cbcrypto.h"
#include "cbsasl_internal.h"

namespace Couchbase {
    class User {
    public:
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
        User()
            : iterationCount(4096),
              dummy(true) {

        }

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
         * Create a new User object with the specified username / password
         * combination. In addition we'll generate a new Salt and a
         * salted SHA1 hashed password.
         *
         * @param unm the username to use
         * @param passwd the password to use
         */
        User(const std::string& unm, const std::string& passwd);

        /**
         * Get the username for this entry
         */
        const std::string& getUsername() const {
            return username;
        }

        /**
         * Get the raw password in plain text
         *
         * @return plain text password
         */
        const std::string& getPlaintextPassword() const {
            return plaintextPassword;
        }

        /**
         * Get the salt used to generate the salted sha1 version of the
         * password. The salt value is stored (and returned) base64 encoded
         * given that all use of the salt on the server side (where this
         * method is used) only use the base64 encoded value.
         *
         * @return base64 encoded version of the salt
         */
        const std::string& getSha1Salt() const {
            return sha1Salt;
        }

        /**
         * Get the Salted SHA1 digest
         *
         * @return salted sha1 password
         */
        const std::vector<uint8_t>& getSaltedSha1Password() const {
            return saltedSha1Password;
        }


        /**
         * Get the salt used to generate the salted sha256 version of the
         * password. The salt value is stored (and returned) base64 encoded
         * given that all use of the salt on the server side (where this
         * method is used) only use the base64 encoded value.
         *
         * @return base64 encoded version of the salt
         */
        const std::string& getSha256Salt() const {
            return sha256Salt;
        }

        /**
         * Get the Salted SHA256 digest
         *
         * @return salted sha256 password
         */
        const std::vector<uint8_t>& getSaltedSha256Password() const {
            return saltedSha256Password;
        }

        /**
         * Get the salt used to generate the salted sha512 version of the
         * password. The salt value is stored (and returned) base64 encoded
         * given that all use of the salt on the server side (where this
         * method is used) only use the base64 encoded value.
         *
         * @return base64 encoded version of the salt
         */
        const std::string& getSha512Salt() const {
            return sha512Salt;
        }

        /**
         * Get the Salted SHA512 digest
         *
         * @return salted sha512 password
         */
        const std::vector<uint8_t>& getSaltedSha512Password() const {
            return saltedSha512Password;
        }

        /**
         * Get the iteration count used for the hashing
         *
         * @return the iteration count used in PKCS5_PBKDF2_HMAC
         */
        int getIterationCount() const {
            return iterationCount;
        }

        /**
          * Is this a dummy object or not?
          */
        bool isDummy() const {
            return dummy;
        }

    protected:
        std::string username;

        std::string plaintextPassword;

        std::string sha1Salt;

        std::vector<uint8_t> saltedSha1Password;

        std::string sha256Salt;

        std::vector<uint8_t> saltedSha256Password;

        std::string sha512Salt;

        std::vector<uint8_t> saltedSha512Password;

        int iterationCount;

        bool dummy;
    };
}
