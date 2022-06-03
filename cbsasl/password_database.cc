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
    if (json.size() != 1) {
        throw std::runtime_error("PasswordDatabase: format error..");
    }

    auto it = json.find("users");
    if (it == json.end()) {
        throw std::runtime_error(
                "PasswordDatabase: format error. users not"
                " present");
    }

    if (!it->is_array()) {
        throw std::runtime_error(
                "PasswordDatabase: Illegal type for \"users\". Expected Array");
    }

    // parse all of the users
    for (const auto& u : *it) {
        User user(u);
        db[user.getUsername().getRawValue()] = user;
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
    nlohmann::json array = nlohmann::json::array();

    for (const auto& u : db) {
        array.push_back(u.second.to_json());
    }
    json["users"] = array;
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
