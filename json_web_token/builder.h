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
#include <chrono>
#include <memory>
#include <string_view>

namespace cb::jwt {

/**
 * Utility class to build up a JWT token
 */
class Builder {
public:
    virtual ~Builder() = default;

    /**
     * Create a new instance of a JWT token builder
     * @param alg The algorithm to sign the token
     * @param passphrase The passphrase used for signing
     * @param payload An optional payload to initialize the builder with
     * @return A new builder to use
     */
    [[nodiscard]] static std::unique_ptr<Builder> create(
            std::string_view alg = "none",
            std::string_view passphrase = {},
            nlohmann::json payload = nlohmann::json::object());
    /// Set the expiration for the token to use
    virtual void setExpiration(std::chrono::system_clock::time_point exp) = 0;
    /// Specify when we can start using the token
    virtual void setNotBefore(std::chrono::system_clock::time_point nbf) = 0;
    /// Specify the time the token was issued
    virtual void setIssuedAt(std::chrono::system_clock::time_point iat) = 0;
    /// Set the JWT identified
    virtual void setJwtId(std::string_view value) = 0;
    /// Add an audience to the token
    virtual void addAudience(std::string_view audience) = 0;
    /// Set the subject on the token
    virtual void setSubject(std::string_view subject) = 0;
    /// Add a (private) claim to the token
    virtual void addClaim(std::string_view name, std::string_view value) = 0;

    /// Build the token
    [[nodiscard]] virtual std::string build() = 0;

    /// Clone this builder
    [[nodiscard]] virtual std::unique_ptr<Builder> clone() const = 0;
};
} // namespace cb::jwt
