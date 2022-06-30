/*
 *     Copyright 2016-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#include <cbsasl/password_database.h>
#include <nlohmann/json.hpp>
#include <platform/dirutils.h>
#include <chrono>
#include <iostream>
#include <memory>
#include <string>

namespace cb::sasl::pwdb {

PasswordDatabase::PasswordDatabase(const nlohmann::json& json) {
    if (!json.contains("@@version@@")) {
        throw std::runtime_error(
                "PasswordDatabase(): @@version@@ attribute not found");
    }

    if (!json["@@version@@"].is_number()) {
        throw std::runtime_error(
                "PasswordDatabase(): @@version@@ must be a number");
    }

    if (json["@@version@@"].get<int>() != 2) {
        throw std::runtime_error(
                "PasswordDatabase(): Unknown version: " +
                std::to_string(json["@@version@@"].get<int>()));
    }

    for (auto it = json.begin(); it != json.end(); ++it) {
        const std::string username = it.key();
        if (username != "@@version@@") {
            db.emplace(username, User(it.value(), UserData{username}));
        }
    }
}

User PasswordDatabase::find(const std::string& username) const {
    auto it = db.find(username);
    if (it == db.end()) {
        // Return a dummy user (allow the authentication to go
        // through the entire authentication phase but fail with
        // incorrect password ;-)
        return User();
    } else {
        return it->second;
    }
}

/// Iterate over all of the users in the database
void PasswordDatabase::iterate(
        std::function<void(const cb::sasl::pwdb::User&)> usercallback) const {
    for (const auto& entry : db) {
        usercallback(entry.second);
    }
}

nlohmann::json PasswordDatabase::to_json() const {
    nlohmann::json json;
    json["@@version@@"] = 2;

    for (const auto& [u, e] : db) {
        json[u] = e.to_json();
    }
    return json;
}

std::string PasswordDatabase::to_string() const {
    return to_json().dump();
}

void MutablePasswordDatabase::upsert(User user) {
    const auto name = user.getUsername().getRawValue();
    db[name] = std::move(user);
}

void MutablePasswordDatabase::remove(const std::string& username) {
    auto it = db.find(username);
    if (it != db.end()) {
        db.erase(it);
    }
}

} // namespace cb::sasl::pwdb
