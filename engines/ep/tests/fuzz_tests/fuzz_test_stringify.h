/*
 *     Copyright 2025-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#pragma once

#include <absl/strings/str_format.h>
#include <fmt/format.h>
#include <fmt/ostream.h> // support operator<<
#include <fmt/ranges.h> // support for std::vector and std::tuple
#include <fmt/std.h> // support for additional types in std::

template <typename T>
concept Formattable = fmt::has_formatter<T, fmt::format_context>::value;

/**
 * Bridge between fmt::format and absl::Format.
 *
 * This is used to format types that are not supported by absl::Format, but
 * are supported by fmt::format.
 *
 * @param sink The sink to write the formatted string to.
 * @param t The value to format.
 */
template <typename Sink, Formattable T>
void AbslStringify(Sink& sink, const T& t) {
    static_assert(Formattable<T>,
                  "Google FuzzTest needs a formatter for this type!");
    absl::Format(&sink, "%s", fmt::to_string(t));
}
