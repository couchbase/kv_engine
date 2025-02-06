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

#include "json_utilities.h"

#include <algorithm>
#include <optional>

namespace cb {

void jsonSetStringView(nlohmann::json& obj,
                       std::string_view key,
                       std::string_view value) {
    auto& target = obj[key];
    if (target.is_string()) {
        // We already has a std::string. Assign the string_view to it directly.
        // This may allow us to re-use the std::string buffer and avoids
        // wrapping the value in a nlohmann::json for operator=().
        target.template get_ref<std::string&>() = value;
    } else {
        target = value;
    }
}

void jsonResetValues(nlohmann::json& obj) {
    for (auto& j : obj) {
        j.clear();
    }
}

void jsonRemoveEmptyStrings(nlohmann::json& obj) {
    obj.erase(std::ranges::remove_if(
                      obj,
                      [](auto& v) {
                          auto* s = v.template get_ptr<std::string*>();
                          return s && s->empty();
                      })
                      .begin(),
              obj.end());
}

std::optional<nlohmann::json> getOptionalJsonObject(
        const nlohmann::json& object, const std::string& key) {
    try {
        return object.at(key);
    } catch (const nlohmann::json::exception&) {
        return {};
    }
}

std::optional<nlohmann::json> getOptionalJsonObject(
        const nlohmann::json& object,
        const std::string& key,
        nlohmann::json::value_t expectedType) {
    auto itr = object.find(key);
    if (itr == object.end()) {
        return {};
    }
    throwIfWrongType(key, *itr, expectedType, "");
    return *itr;
}

nlohmann::json getJsonObject(const nlohmann::json& object,
                             const std::string& key,
                             nlohmann::json::value_t expectedType,
                             const std::string& calledFrom) {
    auto itr = object.find(key);
    if (itr != object.end()) {
        throwIfWrongType(key, *itr, expectedType, calledFrom);
        return *itr;
    }
    std::string err;
    if (!calledFrom.empty()) {
        err.append(calledFrom + ": ");
    }
    err.append("cannot find key:" + key);
    throw std::invalid_argument(err);
}

void throwIfWrongType(const std::string& errorKey,
                      const nlohmann::json& object,
                      nlohmann::json::value_t expectedType,
                      const std::string& calledFrom) {
    if (object.type() != expectedType) {
        std::string err;
        if (!calledFrom.empty()) {
            err.append(calledFrom + ": ");
        }
        err.append("wrong type for key:" + errorKey + ", " + object.dump());
        throw std::invalid_argument(err);
    }
}
} // namespace cb
