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

#pragma once

#include <memcached/mcd_util-visibility.h>

#include <boost/optional/optional_fwd.hpp>
#include <nlohmann/json.hpp>

namespace cb {
/**
 *  Helper function that returns a boost optional json object using the given
 *  object and key.
 *
 * @param object - root json object
 * @param key - the key for the wanted object
 * @return - json object if it exists, otherwise uninitialized
 */
MCD_UTIL_PUBLIC_API
boost::optional<nlohmann::json> getOptionalJsonObject(
        const nlohmann::json& object, const std::string& key);

/**
 *  Helper function that returns a boost optional json object using the given
 *  object and key. The object must be of the expectedType.
 *
 * @param object - root json object
 * @param key - the key for the wanted object
 * @param expectedType - the objects expected type
 * @return - json object if it exists, otherwise uninitialized
 */
MCD_UTIL_PUBLIC_API
boost::optional<nlohmann::json> getOptionalJsonObject(
        const nlohmann::json& object,
        const std::string& key,
        nlohmann::json::value_t expectedType);

/**
 *  Helper function that returns an (nlohmann) json object using the given
 *  object and key. The object must be of the expectedType. Optionally, the
 *  user can specify a "calledFrom" string that will be prefixed into
 *  exception messages to aid debugging.
 *
 * @param object - root json object
 * @param key - the key for the wanted object
 * @param expectedType - the objects expected type
 * @param calledFrom - optional string for exception logging
 * @return - expected json object
 * @throws - std::invalid_argument if the key is not in the given json, or if
 * the json at the given key is not of the expected type
 */
MCD_UTIL_PUBLIC_API
nlohmann::json getJsonObject(const nlohmann::json& object,
                             const std::string& key,
                             nlohmann::json::value_t expectedType,
                             const std::string& calledFrom = "");

/**
 * Helper function that throws an std::invalid_argument exception if the
 * given json object is not of the specified type. Optionally, the user can
 * specify a "calledFrom" string that will be prefixed into exception
 * messages to aid debugging.
 *
 * @param errorKey - the key of the object, for logging
 * @param object - the json object
 * @param expectedType - the expected type of the json object
 * @param calledFrom - optional string for exception logging
 * @throws - std::invalid_argument if the json object is not of the expected
 * type
 */
MCD_UTIL_PUBLIC_API
void throwIfWrongType(const std::string& errorKey,
                      const nlohmann::json& object,
                      nlohmann::json::value_t expectedType,
                      const std::string& calledFrom = "");
} // namespace cb
