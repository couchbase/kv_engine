/*
 *     Copyright 2024-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "token.h"

#include <cbcrypto/digest.h>
#include <fmt/format.h>
#include <nlohmann/json.hpp>
#include <platform/base64.h>
#include <platform/split_string.h>

using namespace std::string_view_literals;

namespace cb::jwt {
std::unique_ptr<Token> Token::parse(
        std::string_view token,
        const std::function<std::optional<std::string>()>& getPassphrase) {
    auto parts = cb::string::split(token, '.');
    if (parts.size() < 2 || parts.size() > 3) {
        throw std::runtime_error(
                "Token::parse: Invalid format. Should be h.v[.s]");
    }

    auto header = nlohmann::json::parse(cb::base64url::decode(parts[0]));
    auto payload = nlohmann::json::parse(cb::base64url::decode(parts[1]));
    bool verified = false;
    if (parts.size() == 2) {
        if (header.value("alg"sv, "none"sv) != "none"sv) {
            throw std::runtime_error(
                    fmt::format("Token::parse: Invalid format. Missing "
                                "signature for \"{}\"",
                                header["alg"].get<std::string>()));
        }
    } else {
        if (header.value("alg"sv, "none"sv) != "HS256"sv) {
            throw std::runtime_error(
                    fmt::format("Token::parse: Unsupported signature \"{}\"",
                                header["alg"].get<std::string>()));
        }

        std::optional<std::string> passphrase =
                getPassphrase ? getPassphrase() : std::nullopt;

        if (passphrase) {
            const auto data = fmt::format("{}.{}", parts[0], parts[1]);
            const auto signature = base64url::encode(
                    HMAC(crypto::Algorithm::SHA256, *passphrase, data));

            if (signature != parts[2]) {
                throw std::runtime_error(
                        fmt::format("Token::parse: Invalid signature"));
            }
            verified = true;
        }
    }
    return std::make_unique<Token>(
            std::move(header), std::move(payload), verified);
}

bool Token::onlyTimestampsDiffers(const Token& other) const {
    if (header != other.header) {
        // the header is different
        return false;
    }

    nlohmann::json mine = payload;
    nlohmann::json others = other.payload;
    for (const auto& field : {
                 "exp",
                 "nbf",
                 "iat",
         }) {
        mine.erase(field);
        others.erase(field);
    }

    return mine == others;
}
} // namespace cb::jwt
