/*
 *     Copyright 2024-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include <cbsasl/error.h>
#include <fmt/format.h>

std::string cb::sasl::format_as(Error error) {
    using namespace std::string_literals;
    switch (error) {
    case Error::OK:
        return "Success"s;
    case Error::CONTINUE:
        return "Continue"s;
    case Error::FAIL:
        return "Fail"s;
    case Error::BAD_PARAM:
        return "Invalid parameters"s;
    case Error::NO_MEM:
        return "No memory"s;
    case Error::NO_MECH:
        return "No such mechanism"s;
    case Error::NO_USER:
        return "No such user"s;
    case Error::PASSWORD_ERROR:
        return "Invalid password"s;
    case Error::NO_RBAC_PROFILE:
        return "User not defined in Couchbase"s;
    case Error::AUTH_PROVIDER_DIED:
        return "Auth provider died"s;
    }

    return fmt::format("format_as(Error): Unknown error: {}",
                       static_cast<int>(error));
}
