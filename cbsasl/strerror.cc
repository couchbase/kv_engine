/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2016-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include <cbsasl/error.h>
#include <stdexcept>

std::string to_string(cb::sasl::Error error) {
    using Error = cb::sasl::Error;
    switch (error) {
    case Error::OK:
        return "Success";
    case Error::CONTINUE:
        return "Continue";
    case Error::FAIL:
        return "Fail";
    case Error::BAD_PARAM:
        return "Invalid parameters";
    case Error::NO_MEM:
        return "No memory";
    case Error::NO_MECH:
        return "No such mechanism";
    case Error::NO_USER:
        return "No such user";
    case Error::PASSWORD_ERROR:
        return "Invalid password";
    case Error::NO_RBAC_PROFILE:
        return "User not defined in Couchbase";
    case Error::AUTH_PROVIDER_DIED:
        return "Auth provider died";
    }

    throw std::invalid_argument(
            "to_string(cb::sasl::Error error): Unknown error: " +
            std::to_string(int(error)));
}

namespace cb::sasl {
std::ostream& operator<<(std::ostream& os, const Error& error) {
    os << ::to_string(error);
    return os;
}
} // namespace cb::sasl
