/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2016 Couchbase, Inc.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */
#include "password_database.h"

#include <nlohmann/json.hpp>
#include <memory>
#include <string>

namespace cb::sasl::pwdb {

PasswordDatabase::PasswordDatabase(const std::string& content, bool file) {
    nlohmann::json json;

    if (file) {
        auto c = read_password_file(content);
        try {
            json = nlohmann::json::parse(c);
        } catch (const nlohmann::json::exception&) {
            throw std::runtime_error(
                    "PasswordDatabase: Failed to parse the JSON in " + content);
        }
    } else {
        try {
            json = nlohmann::json::parse(content);
        } catch (const nlohmann::json::exception&) {
            throw std::runtime_error(
                    "PasswordDatabase: Failed to parse the supplied JSON");
        }
    }

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
        auto user = UserFactory::create(u);
        db[user.getUsername().getRawValue()] = user;
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

} // namespace cb::sasl::pwdb
