/*
 *     Copyright 2024-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#pragma once

#include <nlohmann/json.hpp>
#include <functional>
#include <optional>
#include <string>

namespace cb::jwt {

/// Simplified view of a Token as described in
///   https://datatracker.ietf.org/doc/html/rfc7519
/// and optionally signed as described in
///   https://datatracker.ietf.org/doc/html/rfc7515
class Token final {
public:
    /**
     * Parse an encoded token
     * @param token the token to parse
     * @param getPassphrase callback method to get the passphrase for a token
     * @return The parsed token
     */
    [[nodiscard]] static std::unique_ptr<Token> parse(
            std::string_view token,
            const std::function<std::optional<std::string>()>& getPassphrase =
                    {});

    /// The (JOSE) header in the token
    const nlohmann::json header;
    /// The payload containing claims
    const nlohmann::json payload;
    /// Set to true if there was a signature and it is verified. False
    /// otherwise (if it was signed, but we didn't have the passphrase etc)
    const bool verified;

    /// Check to see if it is only the timestamps (expiry, not before, issued
    /// at) which differ between this and the other token.
    [[nodiscard]] bool onlyTimestampsDiffers(const Token& other) const;

    Token(nlohmann::json header, nlohmann::json payload, bool verified)
        : header(std::move(header)),
          payload(std::move(payload)),
          verified(verified) {
    }
};
} // namespace cb::jwt
