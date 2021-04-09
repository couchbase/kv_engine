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

#include <optional>

namespace cb {
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
