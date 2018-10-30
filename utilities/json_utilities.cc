/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2018 Couchbase, Inc
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

#include "json_utilities.h"

#include <boost/optional.hpp>

namespace cb {
boost::optional<nlohmann::json> getOptionalJsonObject(
        const nlohmann::json& object, const std::string& key) {
    try {
        return object.at(key);
    } catch (const nlohmann::json::exception&) {
        return {};
    }
}

boost::optional<nlohmann::json> getOptionalJsonObject(
        const nlohmann::json& object,
        const std::string& key,
        nlohmann::json::value_t expectedType) {
    try {
        auto rv = object.at(key);
        throwIfWrongType(key, rv, expectedType, "");
        return rv;
    } catch (const nlohmann::json::exception&) {
        return {};
    }
}

nlohmann::json getJsonObject(const nlohmann::json& object,
                             const std::string& key,
                             nlohmann::json::value_t expectedType,
                             const std::string& calledFrom) {
    try {
        auto rv = object.at(key);
        throwIfWrongType(key, rv, expectedType, calledFrom);
        return rv;
    } catch (const nlohmann::json::exception& e) {
        std::string err;
        if (!calledFrom.empty()) {
            err.append(calledFrom + ": ");
        }
        err.append("cannot find key:" + key + ", e:" + e.what());
        throw std::invalid_argument(err);
    }
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
