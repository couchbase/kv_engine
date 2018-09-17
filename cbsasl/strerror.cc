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
