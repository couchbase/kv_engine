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

#include <fstream>
#include <cJSON_utils.h>
#include <string>
#include <memory>

static std::string readFile(const std::string& filename) {
    std::ifstream myfile(filename.c_str());
    if (myfile.is_open()) {
        std::string str((std::istreambuf_iterator<char>(myfile)),
                        std::istreambuf_iterator<char>());
        myfile.close();
        return str;
    } else {
        throw std::runtime_error("Failed to open: " + filename);
    }
}

Couchbase::PasswordDatabase::PasswordDatabase(const std::string& content,
                                              bool file) {
    unique_cJSON_ptr unique_json;

    if (file) {
        unique_json.reset(cJSON_Parse(readFile(content).c_str()));
    } else {
        unique_json.reset(cJSON_Parse(content.c_str()));
    }

    auto* json = unique_json.get();
    if (json == nullptr) {
        if (file) {
            throw std::runtime_error(
                "PasswordDatabase: Failed to parse the JSON in " +
                content);
        } else {
            throw std::runtime_error(
                "PasswordDatabase: Failed to parse the supplied JSON");
        }
    }

    if (cJSON_GetArraySize(json) != 1) {
        throw std::runtime_error("PasswordDatabase: format error..");
    }

    auto* users = cJSON_GetObjectItem(json, "users");
    if (users == nullptr) {
        throw std::runtime_error("PasswordDatabase: format error. users not"
                                     " present");
    }
    if (users->type != cJSON_Array) {
        throw std::runtime_error("PasswordDatabase: Illegal type for "
                                "\"users\". Expected Array");
    }

    // parse all of the users
    for (auto* u = users->child; u != nullptr; u = u->next) {
        Couchbase::User user(u);
        db[user.getUsername()] = user;
    }
}

unique_cJSON_ptr Couchbase::PasswordDatabase::to_json() const {
    auto* json = cJSON_CreateObject();
    auto* array = cJSON_CreateArray();

    for (const auto &u : db) {
        cJSON_AddItemToArray(array, u.second.to_json().release());
    }
    cJSON_AddItemToObject(json, "users", array);
    return unique_cJSON_ptr(json);
}

std::string Couchbase::PasswordDatabase::to_string() const {
    auto json = to_json();
    char* ptr = cJSON_Print(json.get());
    std::string ret(ptr);
    cJSON_Free(ptr);
    return ret;
}
