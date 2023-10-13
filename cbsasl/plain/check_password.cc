/*
 *     Copyright 2017-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#include "check_password.h"

#include <cbsasl/util.h>
#include <gsl/gsl-lite.hpp>

using cb::crypto::Algorithm;

namespace cb::sasl::plain {
Error check_password(const cb::sasl::pwdb::User& user,
                     std::string_view password) {
    const auto& metadata = user.getPaswordHash();
    std::string generated;

    const auto& algorithm = metadata.getAlgorithm();
    if (algorithm == "argon2id") {
        generated = cb::crypto::pwhash(Algorithm::Argon2id13,
                                       password,
                                       metadata.getSalt(),
                                       metadata.getProperties());
    } else if (algorithm == "pbkdf2-hmac-sha512") {
        generated = cb::crypto::pwhash(Algorithm::SHA512,
                                       password,
                                       metadata.getSalt(),
                                       metadata.getProperties());
    } else {
        generated = cb::crypto::pwhash(
                Algorithm::DeprecatedPlain, password, metadata.getSalt());
    }

    bool success = false;
    const auto& originals = metadata.getPasswords();
    for (const auto& pw : originals) {
        if (pw.size() != generated.size()) {
            return Error::FAIL;
        }

        // Compare the generated with the stored
        if ((cbsasl_secure_compare(generated, pw) ^
             gsl::narrow_cast<int>(user.isDummy())) == 0) {
            success = true;
        }
    }

    return success ? Error::OK : Error::PASSWORD_ERROR;
}

} // namespace cb::sasl::plain
