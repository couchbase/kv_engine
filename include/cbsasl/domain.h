/*
 *     Copyright 2015 Couchbase, Inc.
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

#include <cstdint>
#include <string>

namespace cb::sasl {

/**
 * The Domain enum defines all of the legal states where the users may
 * be defined.
 */
enum class Domain : uint8_t {
    /**
     * The user is defined locally on the node and authenticated
     * through `cbsasl` (or by using SSL certificates)
     */
    Local,
    /**
     * The user is defined somewhere else but authenticated through
     * `saslauthd`
     */
    External
};

Domain to_domain(const std::string& domain);

} // namespace cb::sasl

std::string to_string(cb::sasl::Domain domain);
