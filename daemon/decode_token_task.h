/*
 *     Copyright 2025-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#pragma once

#include "authn_authz_service_task.h"
#include <folly/Synchronized.h>

class Cookie;

/**
 * The DecodeTokenTask is used to request the external auth provider
 * to decode the provided token
 */
class DecodeTokenTask : public AuthnAuthzServiceTask {
public:
    DecodeTokenTask(Cookie& cookie_, const std::string& token_);

    void externalResponse(cb::mcbp::Status status,
                          std::string_view payload) override;

    constexpr std::string_view getMechanism() const {
        using namespace std::string_view_literals;
        return "OAUTHBEARER"sv;
    }

    const auto& getCookie() const {
        return cookie;
    }

    std::string_view getChallenge() const {
        return challenge;
    }

    auto getStatus() const {
        return result.lock()->status;
    }

    auto getData() const {
        return result.lock()->data;
    }

protected:
    Cookie& cookie;
    std::string challenge;
    struct Result {
        std::string data;
        cb::mcbp::Status status;
    };
    folly::Synchronized<Result, std::mutex> result;
};
