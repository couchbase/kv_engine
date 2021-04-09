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

#pragma once

#include <nlohmann/json.hpp>
#include <optional>

namespace cb {

/// The nlohmann exception code for incorrect types
const int nlohmannExceptionTypeCode = 302;

/**
 *  Helper function for throwing nlohmann incorrect type exceptions. Useful
 *  for when we want to throw exception of a consistent type.
 *
 * @param msg the error message to be printed
 */
[[noreturn]] inline void throwJsonTypeError(const std::string& msg) {
    throw nlohmann::detail::type_error::create(nlohmannExceptionTypeCode, msg);
}

/**
 *  Helper function that will allow us to do an
 *  nlohmann::json.at("foo").get<bar>() and return meaningful error
 *  messages from the get call.
 *
 *  The standard .get<bar>() method will throw an exception that tells
 *  the user a value is incorrect, but this does not include the key for
 *  the value so the user may struggle to identify the problematic value.
 *
 *  This function intercepts the exception thrown by get and rethrows the
 *  exception with the key prepended to the message.
 *
 * @tparam T - type of value that we wish to get
 * @param obj - root json object
 * @param key - key at which to retrieve the json value
 * @throws nlohmann::detail::out_of_range if the key does not exist
 * @throws nlohmann::detail::type_error if the value is of an incorrect type
 * @return the value of type T
 */
template <typename T>
T jsonGet(const nlohmann::json& obj, const std::string& key) {
    nlohmann::json value = obj.at(key);
    try {
        return value.get<T>();
    } catch (nlohmann::json::exception& e) {
        throwJsonTypeError("value for key \"" + key + "\" - " + e.what());
    }
}

/**
 *  Alternate helper function that will allows the use of an iterator,
 *  which contains a key and value, to trigger
 *  nlohmann::json::const_iterator.value().get<bar> and return meaningful
 *  error messages from the get call.
 *
 *  The standard .get<bar>() method will throw an exception that tells
 *  the user a value is incorrect, but this does not include the key for
 *  the value so the user may struggle to identify the problematic value.
 *
 *  This function intercepts the exception thrown by get and rethrows the
 *  exception with the key prepended to the message.
 *
 * @tparam T - type of value that we wish to get
 * @param it - iterator json object which contains both a key and value.
 * @throws nlohmann::detail::out_of_range if the key does not exist
 * @throws nlohmann::detail::type_error if the value is of an incorrect type
 * @return the value of type T
 */
template <typename T>
T jsonGet(nlohmann::json::const_iterator it) {
    try {
        return it.value().get<T>();
    } catch (nlohmann::json::exception& e) {
        throwJsonTypeError("value for key \"" + it.key() + "\" - " + e.what());
    }
}

/**
 *  Helper function that returns a std::optional json object using the given
 *  object and key.
 *
 * @param object - root json object
 * @param key - the key for the wanted object
 * @return - json object if it exists, otherwise uninitialized
 */
std::optional<nlohmann::json> getOptionalJsonObject(
        const nlohmann::json& object, const std::string& key);

/**
 *  Helper function that returns a std::optional json object using the given
 *  object and key. The object must be of the expectedType.
 *
 * @param object - root json object
 * @param key - the key for the wanted object
 * @param expectedType - the objects expected type
 * @return - json object if it exists, otherwise uninitialized
 */
std::optional<nlohmann::json> getOptionalJsonObject(
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
void throwIfWrongType(const std::string& errorKey,
                      const nlohmann::json& object,
                      nlohmann::json::value_t expectedType,
                      const std::string& calledFrom = "");
} // namespace cb
