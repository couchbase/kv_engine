/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2017 Couchbase, Inc.
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

#include "check_password.h"

#include <cbsasl/logging.h>
#include <cbsasl/util.h>
#include <utilities/logtags.h>
#include <iterator>

namespace cb::sasl::plain {

/**
 * ns_server generates a (salted) password hash by generating a 16 bytes
 * long salt, which is used with a HMAC and the resulting SHA1 (which is
 * 20 bytes) is appended to the salt and that is the entire password
 */
static const int SALT_SIZE = 16;
static const int HASH_SIZE = 20;
static const std::string::size_type PASSWORD_SIZE = 36;

Error check_password(Context* context,
                     const cb::sasl::pwdb::User& user,
                     const std::string& password) {
    const auto storedPassword = user.getPassword(Mechanism::PLAIN).getPassword();
    const auto size = storedPassword.size();
    if (size != PASSWORD_SIZE) {
        std::string message{
                "cb::cbsasl::check_password: Invalid password entry for [" +
                user.getUsername().getSanitizedValue() + "]"};
        logging::log(context, logging::Level::Error, message);
        return Error::FAIL;
    }


    // Copy out the resulting HMAC stored in the password
    std::string stored_hmac;
    std::copy(storedPassword.data() + SALT_SIZE,
              storedPassword.data() + PASSWORD_SIZE,
              std::back_inserter(stored_hmac));

    // Use the same salt as stored with the password
    std::string salt;
    std::copy(storedPassword.data(),
              storedPassword.data() + SALT_SIZE,
              std::back_inserter(salt));

    // Try to generate the same HMAC as we've got stored by using the
    // same salt and the users provided password
    auto generated_hmac =
            cb::crypto::HMAC(cb::crypto::Algorithm::SHA1, salt, password);

    // Compare the entire generated HMAC with the user provided HMAC
    bool same = !user.isDummy();
    for (int ii = 0; ii < HASH_SIZE; ++ii) {
        if (stored_hmac[ii] != generated_hmac[ii]) {
            // we don't want an early exit...
            same = false;
        }
    }

    if (same) {
        return Error::OK;
    }
    return Error::PASSWORD_ERROR;
}

} // namespace cb::sasl::plain
