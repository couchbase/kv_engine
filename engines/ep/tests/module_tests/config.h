/*
 *     Copyright 2022-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#pragma once

#include <folly/portability/GTest.h>
#include <map>
#include <set>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace config {

/**
 * Represents a set of engine configurations.
 *
 * The set of configurations represented as strings can be obtained by calling
 * Config::toStrings().
 *
 * Two primitive operations are supported:
 * - Cartesian product of two sets of configurations - operator* or .combine()
 * - Concatenation of two sets of configurations - operator| or .join()
 */
class Config {
private:
    /// Represents a single engine config.
    using Params = std::map<std::string, std::string>;
    /// Represents a set of engine configs.
    using ParamLists = std::set<Params>;

public:
    /**
     * The return type of ::testing::ValuesIn(this->toString()).
     */
    using GTestGeneratorType = decltype(
            ::testing::ValuesIn(std::declval<std::vector<std::string>>()));

    struct Param {
        const std::string key;
        const std::set<std::string> values;

        Param(std::string_view key, std::string_view value)
            // GCC complains about -Wmissing-braces for values with single {}
            : key(key), values({{std::string(value)}}) {
        }

        Param(std::string_view key,
              std::initializer_list<std::string_view> values)
            : key(key), values(values.begin(), values.end()) {
        }
    };

    /**
     * An object containing 0 configurations.
     */
    Config() = default;

    /**
     * An object containing 1 configuration with the specified parameters.
     */
    Config(std::initializer_list<Param> params);

    Config(Config&&) = default;
    Config(const Config&) = default;

    Config& operator=(const Config&) = default;
    Config& operator=(Config&&) = default;

    /**
     * Adds a property with the specified values to all configurations.
     * Generates one configuration per value specified.
     *
     * @param key The key for the property
     * @param values The different values that the property should have
     * @return *this
     */
    Config& add(std::string_view key, const std::set<std::string>& values);

    /**
     * Generates the string representation of the engine configurations as
     * a vector of strings of the form key=value:key2=value2.
     */
    std::vector<std::string> toStrings() const;

    /**
     * Implicitly convert to the result of ::testing::ValuesIn(toString()) so
     * we can use this type in parametric test declarations directly.
     */
    operator GTestGeneratorType() const;

    /**
     * Creates a Config representing the union of the configurations in *this
     * and other.
     */
    Config join(const Config& other) const;

    /**
     * Creates a Config representing the Cartesian product of the configurations
     * in *this and other.
     */
    Config combine(const Config& other) const;

private:
    static std::string createConfigurationString(const Params& params);
    ParamLists paramLists;
};

/**
 * Creates a Config representing the union of the configurations in *this
 * and other.
 */
Config operator|(const Config& lhs, const Config& rhs);

/**
 * Creates a Config representing the Cartesian product of the configurations
 * in *this and other.
 */
Config operator*(const Config& lhs, const Config& rhs);

} // namespace config