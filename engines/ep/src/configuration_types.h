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

} // namespace cb::config
