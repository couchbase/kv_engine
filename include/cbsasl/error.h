/*
 *     Copyright 2018-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#pragma once

#include <string>

namespace cb::sasl {

/**
 * The error values used in CBSASL
 */
enum class Error {
    OK,
    CONTINUE,
    FAIL,
    BAD_PARAM,
    NO_MEM,
    NO_MECH,
    NO_USER,
    PASSWORD_ERROR,
    NO_RBAC_PROFILE,
    AUTH_PROVIDER_DIED
};
std::ostream& operator<<(std::ostream& os, const Error& error);
} // namespace cb::sasl

std::string to_string(cb::sasl::Error error);
