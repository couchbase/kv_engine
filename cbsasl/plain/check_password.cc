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
#include "../cbcrypto.h"

#include <cbsasl/logging.h>
#include <platform/base64.h>

using cb::crypto::Algorithm;

namespace cb::sasl::plain {
Error check_password(Context* context,
                     const cb::sasl::pwdb::User& user,
                     const std::string& password) {
    const auto& metadata = user.getPaswordHash();
    std::string generated;

    if (metadata.getAlgorithm() == "argon2id") {
        generated = cb::crypto::pwhash(Algorithm::Argon2id13,
                                       password,
                                       metadata.getSalt(),
                                       metadata.getProperties());
    } else {
        generated = cb::crypto::pwhash(
                Algorithm::DeprecatedPlain, password, metadata.getSalt());
    }

    const auto& original = metadata.getPassword();
    if (original.size() != generated.size()) {
        return Error::FAIL;
    }

    // Compare the generated with the stored
    bool same = !user.isDummy();
    for (std::size_t ii = 0; ii < original.size(); ++ii) {
        if (generated[ii] != original[ii]) {
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
