/*
 *     Copyright 2015-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
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
