/*
 *     Copyright 2024-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#pragma once

#include <atomic>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>

namespace cb::config {
#include "generated_enums.h" // NOLINT(*)

namespace detail {

template <typename T>
auto isConfigurationEnumHelper(int)
        -> decltype(cb::config::from_string(std::declval<T&>(), ""),
                    std::true_type{});

template <typename T>
std::false_type isConfigurationEnumHelper(...);

template <typename T>
static constexpr bool isConfigurationEnum =
        decltype(isConfigurationEnumHelper<T>(0))::value;

} // namespace detail

template <typename T,
          typename = std::enable_if_t<detail::isConfigurationEnum<T>>>
std::string to_string(T val) {
    return std::string(cb::config::format_as(val));
}

/**
 * A feature version is a major and minor version.
 *
 * Supports comparison and ordering.
 */
struct FeatureVersion {
    /**
     * The maximum feature version.
     */
    static FeatureVersion max();
    /**
     * Parses a feature version from a string in the format "major.minor".
     *
     * @param version the input to parse
     * @return the parsed feature version
     * @throws std::invalid_argument
     */
    static FeatureVersion parse(std::string_view version);

    /// Construct a feature version.
    FeatureVersion(uint8_t major, uint8_t minor);

    FeatureVersion(const FeatureVersion&) = default;
    FeatureVersion& operator=(const FeatureVersion&) = default;

    /// Compare two feature versions.
    std::strong_ordering operator<=>(const FeatureVersion&) const noexcept;
    bool operator==(const FeatureVersion&) const noexcept;

    /// The major version
    uint8_t major_version{};
    /// The minor version
    uint8_t minor_version{};
};

static_assert(std::atomic<FeatureVersion>::is_always_lock_free,
              "cb::config::FeatureVersion atomic is not lock-free");

std::string format_as(const FeatureVersion& version);

enum class ExcludeWhenValueIsDefaultValue { Yes, No };

} // namespace cb::config
