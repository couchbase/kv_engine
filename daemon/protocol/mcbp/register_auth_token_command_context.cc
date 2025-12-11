/*
 *     Copyright 2025-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#include "register_auth_token_command_context.h"

#include "daemon/external_auth_manager_thread.h"

#include <daemon/connection.h>
#include <daemon/cookie.h>

RegisterAuthTokenCommandContext::RegisterAuthTokenCommandContext(Cookie& cookie)
    : SteppableCommandContext(cookie) {
    auto json = nlohmann::json::parse(cookie.getRequest().getValueString());
    id = json["id"].get<uint16_t>();
    token = json.value("token", std::string{});
    if (token.empty()) {
        state = State::RemoveId;
    } else {
        state = State::AddId;
    }
}
cb::engine_errc RegisterAuthTokenCommandContext::step() {
    auto ret = cb::engine_errc::success;
    while (ret == cb::engine_errc::success) {
        switch (state) {
        case State::RemoveId:
            ret = remove_id();
            break;
        case State::AddId:
            decodeTokenTask = std::make_shared<DecodeTokenTask>(cookie, token);
            externalAuthManager->enqueueRequest(*decodeTokenTask);
            cookie.setEwouldblock();
            state = State::WaitForDecode;
            return cb::engine_errc::would_block;
        case State::WaitForDecode:
            ret = add_id();
            break;
        case State::Done:
            Expects(!cookie.isEwouldblock());
            cookie.sendResponse(cb::engine_errc::success);
            return cb::engine_errc::success;
        }
    }
    return ret;
}

cb::engine_errc RegisterAuthTokenCommandContext::remove_id() {
    if (cookie.getConnection().removeTokenAuthDataById(id)) {
        state = State::Done;
        return cb::engine_errc::success;
    }
    return cb::engine_errc::no_such_key;
}

cb::engine_errc RegisterAuthTokenCommandContext::add_id() {
    auto data = decodeTokenTask->getData();

    if (decodeTokenTask->getStatus() != cb::mcbp::Status::Success) {
        return cb::engine_errc::failed;
    }

    nlohmann::json json;
    try {
        json = nlohmann::json::parse(data);
    } catch (const std::exception&) {
        return cb::engine_errc::failed;
    }

    if (!json["token"].contains("rbac")) {
        cookie.setErrorContext("Internal error. No rbac entry");
        return cb::engine_errc::failed;
    }

    auto rbac = json["token"]["rbac"];
    std::string username;
    int count = 0;
    for (auto it = rbac.begin(); it != rbac.end(); ++it) {
        username = it.key();
        ++count;
    }

    if (!(count == 1 && !username.empty())) {
        cookie.setErrorContext("Internal error. Failed to locate username");
        return cb::engine_errc::failed;
    }

    json = json["token"];
    Expects(json.contains("rbac"));
    std::optional<std::chrono::system_clock::time_point> lifetimeBegin;
    std::optional<std::chrono::system_clock::time_point> lifetimeEnd;
    if (json.contains("nbf")) {
        lifetimeBegin = std::chrono::system_clock::from_time_t(json["nbf"]);
    }
    if (json.contains("exp")) {
        lifetimeEnd = std::chrono::system_clock::from_time_t(json["exp"]);
    }

    const cb::rbac::UserIdent user{
            username, cb::sasl::to_domain(json.value("domain", "external"))};
    connection.upsertTokenAuthDataById(
            id,
            user,
            std::make_unique<cb::rbac::UserEntry>(
                    user.name, json["rbac"][user.name], user.domain),
            lifetimeBegin,
            lifetimeEnd);
    state = State::Done;
    return cb::engine_errc::success;
}
