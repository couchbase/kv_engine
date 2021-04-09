/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2018-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#include "auth_provider.h"

#include <mcbp/protocol/request.h>
#include <mcbp/protocol/status.h>
#include <nlohmann/json.hpp>
#include <platform/base64.h>

std::pair<cb::mcbp::Status, std::string> AuthProvider::process(
        const cb::mcbp::Request& req) {
    switch (req.getServerOpcode()) {
    case cb::mcbp::ServerOpcode::ClustermapChangeNotification:
    case cb::mcbp::ServerOpcode::ActiveExternalUsers:
        // not supported
        break;
    case cb::mcbp::ServerOpcode::Authenticate:
        return processAuthnRequest(req.getValue());

    case cb::mcbp::ServerOpcode::GetAuthorization:
        return processAuthzRequest(req.getKey());
    }
    throw std::runtime_error("AuthProvider::process: unsupported opcode");
}

std::pair<cb::mcbp::Status, std::string> AuthProvider::processAuthzRequest(
        const std::string& user) {
    auto ret = getUserEntry(user);
    switch (ret.first) {
    case cb::sasl::Error::OK:
        break;
    case cb::sasl::Error::CONTINUE:
    case cb::sasl::Error::BAD_PARAM:
    case cb::sasl::Error::NO_MEM:
    case cb::sasl::Error::NO_MECH:
    case cb::sasl::Error::FAIL:
    case cb::sasl::Error::AUTH_PROVIDER_DIED:
    case cb::sasl::Error::NO_USER:
    case cb::sasl::Error::PASSWORD_ERROR:
        throw std::runtime_error(
                "AuthProvider::processAuthzRequest: Invalid return value from "
                "getUserEntry");

    case cb::sasl::Error::NO_RBAC_PROFILE:
        return std::make_pair<cb::mcbp::Status, std::string>(
                cb::mcbp::Status::AuthError, {});
    }

    nlohmann::json payload;
    payload["rbac"] = ret.second;
    return std::pair<cb::mcbp::Status, std::string>(cb::mcbp::Status::Success,
                                                    payload.dump());
}

std::pair<cb::mcbp::Status, std::string> AuthProvider::processAuthnRequest(
        const std::string& request) {
    const auto json = nlohmann::json::parse(request);
    return start(json.at("mechanism").get<std::string>(),
                 json.at("challenge").get<std::string>(),
                 json.at("authentication-only").get<bool>());
}

std::pair<cb::mcbp::Status, std::string> AuthProvider::start(
        const std::string& mechanism,
        const std::string& challenge,
        bool authOnly) {
    if (mechanism != "PLAIN") {
        return std::make_pair<cb::mcbp::Status, std::string>(
                cb::mcbp::Status::NotSupported,
                R"({"error":{"context":"mechanism not supported"}})");
    }

    const auto ch = cb::base64::decode(challenge);
    return plain_auth({reinterpret_cast<const char*>(ch.data()), ch.size()},
                      authOnly);
}

std::pair<cb::mcbp::Status, std::string> AuthProvider::plain_auth(
        std::string_view input, bool authOnly) {
    // The syntax for the payload for plain auth is a string looking like:
    // \0username\0password
    if (input.empty()) {
        return std::make_pair<cb::mcbp::Status, std::string>(
                cb::mcbp::Status::Einval, {});
    }

    // Skip everything up to the first \0
    size_t inputpos = 0;
    while (inputpos < input.size() && input[inputpos] != '\0') {
        inputpos++;
    }
    inputpos++;

    if (inputpos >= input.size()) {
        return std::make_pair<cb::mcbp::Status, std::string>(
                cb::mcbp::Status::Einval, {});
    }

    const char* ptr = input.data() + inputpos;
    while (inputpos < input.size() && input[inputpos] != '\0') {
        inputpos++;
    }
    inputpos++;

    if (inputpos > input.size()) {
        return std::make_pair<cb::mcbp::Status, std::string>(
                cb::mcbp::Status::Einval, {});
    }

    const std::string username(ptr);
    std::string password;

    if (inputpos != input.size()) {
        size_t pwlen = 0;
        ptr = input.data() + inputpos;
        while (inputpos < input.size() && input[inputpos] != '\0') {
            inputpos++;
            pwlen++;
        }
        password = std::string{ptr, pwlen};
    }

    const auto ret = validatePassword(username, password);
    switch (ret.first) {
    case cb::sasl::Error::OK:
        break;
    case cb::sasl::Error::CONTINUE:
        throw std::runtime_error(
                "AuthProvider::plain_auth: PLAIN auth does not support "
                "stepping");

    case cb::sasl::Error::BAD_PARAM:
    case cb::sasl::Error::NO_MEM:
    case cb::sasl::Error::NO_MECH:
    case cb::sasl::Error::FAIL:
    case cb::sasl::Error::AUTH_PROVIDER_DIED:
        throw std::runtime_error(
                "AuthProvider::plain_auth: Invalid return value from password "
                "validator");

    case cb::sasl::Error::NO_RBAC_PROFILE:
        return std::make_pair<cb::mcbp::Status, std::string>(
                cb::mcbp::Status::AuthError, {});
    case cb::sasl::Error::NO_USER:
        return std::make_pair<cb::mcbp::Status, std::string>(
                cb::mcbp::Status::KeyEnoent, {});
    case cb::sasl::Error::PASSWORD_ERROR:
        return std::make_pair<cb::mcbp::Status, std::string>(
                cb::mcbp::Status::KeyEexists, {});
    }

    nlohmann::json payload;
    if (!authOnly) {
        payload["rbac"] = ret.second;
    }

    return std::pair<cb::mcbp::Status, std::string>(cb::mcbp::Status::Success,
                                                    payload.dump());
}
