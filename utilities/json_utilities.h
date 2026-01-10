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

#include <fmt/format.h>
#include <nlohmann/json.hpp>
#include <platform/string_hex.h>

#include <charconv>
#include <optional>

namespace cb {

/**
 *  Helper function for throwing nlohmann incorrect type exceptions. Useful
 *  for when we want to throw exception of a consistent type.
 *
 * @param msg the error message to be printed
 */
[[noreturn]] inline void throwJsonTypeError(const std::string& msg) {
    const int nlohmannExceptionTypeCode = 302;
    throw nlohmann::detail::type_error::create(
            nlohmannExceptionTypeCode, msg, nullptr);
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

/**
 * Helper function to extract a value of type T from a json object
 * given a key. If the key does not exist or the type is incorrect
 * an exception is thrown. It allows the number to either be a JSON
 * number, a string containing a decimal number, or a string containing
 * a hex number prefixed with "0x".
 *
 * @param json the json object to extract from
 * @param key the key to extract
 * @return the extracted value
 * @throws std::logic_error if the key does not exist or if its not a number
 *                          or string
 * @throws std::out_of_range if the value won't fit in the provided datatype
 */
template <typename T>
static T getValueFromJson(const nlohmann::json& json, const std::string& key) {
    if (!json.contains(key)) {
        throw std::logic_error(
                "cb::getValueFromJson(): Missing required field: " + key);
    }

    if (json[key].is_number()) {
        return json[key].get<T>();
    }

    if (json[key].is_string()) {
        std::string value = json[key].get<std::string>();
        if (value.starts_with("0x")) {
            return cb::from_hex(value);
        }

        T ret;
        const auto [ptr, ec]{std::from_chars(
                value.data(), value.data() + value.size(), ret)};
        if (ec != std::errc()) {
            if (ec == std::errc::result_out_of_range) {
                throw std::out_of_range(
                        fmt::format("cb::getValueFromJson(): Value for '{}' "
                                    "won't fit in the datatype",
                                    key));
            }
            throw std::system_error(std::make_error_code(ec));
        }
        return ret;
    }

    throw std::logic_error(fmt::format(
            "cb::getValueFromJson(): Field '{}' must be a number: '{}'",
            key,
            json[key].dump()));
}

} // namespace cb
