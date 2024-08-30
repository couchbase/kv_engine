/*
 *     Copyright 2024-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "builder.h"

#include <cbcrypto/digest.h>
#include <fmt/format.h>
#include <nlohmann/json.hpp>
#include <platform/base64.h>

namespace cb::jwt {
class BuilderImpl : public Builder {
public:
    explicit BuilderImpl() {
        header = {{"typ", "JWT"}};
        payload = {{"iss", "cb-unit-tests"}};
    }

    void setExpiration(std::chrono::system_clock::time_point exp) override {
        payload["exp"] = std::chrono::system_clock::to_time_t(exp);
    }
    void setNotBefore(std::chrono::system_clock::time_point nbf) override {
        payload["nbf"] = std::chrono::system_clock::to_time_t(nbf);
    }
    void setIssuedAt(std::chrono::system_clock::time_point iat) override {
        payload["iat"] = std::chrono::system_clock::to_time_t(iat);
    }
    void setJwtId(std::string_view value) override {
        payload["jti"] = value;
    }
    void setSubject(std::string_view subject) override {
        payload["sub"] = subject;
    }
    void addAudience(std::string_view audience) override {
        addClaim("aud", audience);
    }
    void addClaim(std::string_view name, std::string_view value) override {
        if (payload.contains(name)) {
            if (payload[name].is_string()) {
                auto old = payload[name].get<std::string>();
                payload[name] = nlohmann::json::array();
                payload[name].push_back(old);
            }
            payload[name].push_back(value);
            return;
        }
        payload[name] = value;
    }
    std::string build() override {
        return fmt::format("{}.{}",
                           cb::base64url::encode(header.dump()),
                           cb::base64url::encode(payload.dump()));
    }

    nlohmann::json header;
    nlohmann::json payload;
};

class PlainBuilderImpl : public BuilderImpl {
public:
    explicit PlainBuilderImpl() {
        header["alg"] = "none";
    }
};

class HS256BuilderImpl : public BuilderImpl {
public:
    explicit HS256BuilderImpl(std::string passphrase)
        : passphrase(std::move(passphrase)) {
        header["alg"] = "HS256";
    }

    std::string build() override {
        const auto data = BuilderImpl::build();
        const auto signature = base64url::encode(
                HMAC(crypto::Algorithm::SHA256, passphrase, data));
        return fmt::format("{}.{}", data, signature);
    }

    const std::string passphrase;
};

std::unique_ptr<Builder> Builder::create(std::string_view alg,
                                         std::string_view passphrase) {
    if (alg == "HS256") {
        return std::make_unique<HS256BuilderImpl>(std::string(passphrase));
    }
    if (alg.empty() || alg == "none") {
        return std::make_unique<PlainBuilderImpl>();
    }
    throw std::invalid_argument("Invalid Algorithm");
}

} // namespace cb::jwt