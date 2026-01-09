/*
 *     Copyright 2026-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#pragma once

/**
 * Define enum class with yes/no values and the typical formatting methods
 *
 * @param T The name of the enum
 */
#define DEFINE_YES_NO_ENUM(T)                                                 \
    enum class T : char { No, Yes };                                          \
    inline auto format_as(const T value) {                                    \
        switch (value) {                                                      \
        case T::No:                                                           \
            return #T "::No";                                                 \
        case T::Yes:                                                          \
            return #T "::Yes";                                                \
        }                                                                     \
        throw std::invalid_argument("format_as(" #T ") unknown " +            \
                                    std::to_string(static_cast<int>(value))); \
    }                                                                         \
    inline std::ostream& operator<<(std::ostream& os, const T& value) {       \
        return os << format_as(value);                                        \
    }                                                                         \
    inline std::string to_string(const T value) {                             \
        return format_as(value);                                              \
    }
