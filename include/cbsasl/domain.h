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

#include <nlohmann/json_fwd.hpp>
#include <cstdint>
#include <string>
#include <string_view>

namespace cb::sasl {

/**
 * The Domain enum defines all of the legal states where the users may
 * be defined.
 */
enum class [[nodiscard]] Domain : uint8_t {
    /**
     * The user is defined locally on the node and authenticated
     * through `cbsasl` (or by using SSL certificates)
     */
    Local,
    /**
     * The user is defined somewhere else but authenticated through
     * `saslauthd`
     */
    External,
    /// The domain is unknown
    Unknown,
};

Domain to_domain(std::string_view domain);
std::string format_as(Domain domain);
void to_json(nlohmann::json& json, Domain domain);
} // namespace cb::sasl
