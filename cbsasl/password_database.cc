/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2016-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#include "password_database.h"

#include <nlohmann/json.hpp>
#include <platform/dirutils.h>
#include <chrono>
#include <fstream>
#include <iostream>
#include <memory>
#include <sstream>
#include <string>

namespace cb::sasl::pwdb {

std::string PasswordDatabase::read_password_file(const std::string& filename) {
    if (filename == "-") {
        std::string contents;
        try {
            contents.assign(std::istreambuf_iterator<char>{std::cin},
                            std::istreambuf_iterator<char>());
        } catch (const std::exception& ex) {
            std::cerr << "Failed to read password database from stdin: "
                      << ex.what() << std::endl;
            exit(EXIT_FAILURE);
        }
        return contents;
    }

    auto ret = cb::io::loadFile(filename, std::chrono::seconds{5});

    // The password file may be encrypted
    auto* env = getenv("COUCHBASE_CBSASL_SECRETS");
    if (env == nullptr) {
        return ret;
    }

    nlohmann::json json;
    try {
        json = nlohmann::json::parse(env);
    } catch (const nlohmann::json::exception& e) {
        std::stringstream ss;
        ss << "cbsasl_read_password_file: Invalid json specified in "
              "COUCHBASE_CBSASL_SECRETS. Parse failed with: '"
           << e.what() << "'";
        throw std::runtime_error(ss.str());
    }

    return cb::crypto::decrypt(json, ret);
}

void PasswordDatabase::write_password_file(const std::string& filename,
                                           const std::string& content) {
    if (filename == "-") {
        std::cout.write(content.data(), content.size());
        std::cout.flush();
        return;
    }

    std::ofstream of(filename, std::ios::binary);

    auto* env = getenv("COUCHBASE_CBSASL_SECRETS");
    if (env == nullptr) {
        of.write(content.data(), content.size());
    } else {
        nlohmann::json json;
        try {
            json = nlohmann::json::parse(env);
        } catch (const nlohmann::json::exception& e) {
            std::stringstream ss;
            ss << "cbsasl_write_password_file: Invalid json specified in "
                  "COUCHBASE_CBSASL_SECRETS. Parse failed with: '"
               << e.what() << "'";
            throw std::runtime_error(ss.str());
        }

        auto enc = cb::crypto::encrypt(json, content);
        of.write(enc.data(), enc.size());
    }

    of.flush();
    of.close();
}

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
        User user(u);
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
