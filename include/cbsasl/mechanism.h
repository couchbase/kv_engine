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

#include <cbsasl/error.h>

#include <stdexcept>
#include <string>

namespace cb::sasl {

enum class Mechanism {
    OAUTHBEARER,
    SCRAM_SHA512,
    SCRAM_SHA256,
    SCRAM_SHA1,
    PLAIN
};
std::string_view format_as(Mechanism mechanism);

class unknown_mechanism : public std::invalid_argument {
public:
    explicit unknown_mechanism(const std::string msg)
        : std::invalid_argument(msg) {
    }
};

/**
 * Select the preferred mechanism from one of the provided of mechanisms.
 * This method is used from the _clients_ to pick one of the mechanisms
 * returned from the server
 *
 * @param mechanisms the list of mechanisms to choose from
 * @return the mechanism to use
 * @throws unknown_mechanism if no supported mechanism is listed in the
 *                           available mechanisms
 */
Mechanism selectMechanism(std::string_view mechanisms);

/**
 * Select the given mechanism from one of the listed mechanisms. This
 * code is used on the server to select the mechanism the client requested
 * (which might not be available on the server. Either by providing a
 * value we have never heard of, or if the administrator disabled the
 * named mechanism)
 *
 * @param mech The requested mechanism
 * @param mechanisms The mechanisms to choose from
 * @return The mechanism to use
 * @throws unknown_mechanism if mech isn't listed in mechanisms (or not
 *                            supported internally)
 */
Mechanism selectMechanism(std::string_view mech, std::string_view mechanisms);

namespace mechanism::plain {
/**
 * Try to run a plain text authentication with the specified username and
 * password.
 */
Error authenticate(const std::string& username, const std::string& passwd);
} // namespace mechanism::plain
} // namespace cb::sasl

std::string to_string(cb::sasl::Mechanism mechanism);
