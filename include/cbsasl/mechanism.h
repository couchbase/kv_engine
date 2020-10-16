/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2018 Couchbase, Inc.
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

#include <cbsasl/error.h>

#include <stdexcept>
#include <string>

namespace cb::sasl {

enum class Mechanism { SCRAM_SHA512, SCRAM_SHA256, SCRAM_SHA1, PLAIN };

class unknown_mechanism : public std::invalid_argument {
public:
    explicit unknown_mechanism(const std::string& msg)
        : std::invalid_argument(msg) {
    }
};

/**
 * Select a mechanism from one of the listed of mechanisms. This
 * method will pick the "most secure" mechanism.
 *
 * @param mechanisms the list of mechanisms to choose from
 * @return the mechanism to use
 * @throws unknown_mechanism if no supported mechanism is listed in the
 *                           available mechanisms
 */
Mechanism selectMechanism(const std::string& mechanisms);

/**
 * Select the given mechanism from one of the listed mechanisms.
 *
 * @param mech The requested mechanism
 * @param mechanisms The mechanisms to choose from
 * @return The mechanism to use
 * @throws unknown_mechanism if mech isn't listed in mechanisms (or not
 *                           not supported internally.
 */
Mechanism selectMechanism(const std::string& mech,
                          const std::string& mechanisms);

namespace mechanism {
namespace plain {
/**
 * Try to run a plain text authentication with the specified username and
 * password.
 */

Error authenticate(const std::string& username, const std::string& passwd);

} // namespace plain
} // namespace mechanism

} // namespace cb::sasl

std::string to_string(cb::sasl::Mechanism mechanism);
