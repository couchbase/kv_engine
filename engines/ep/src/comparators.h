/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2020 Couchbase, Inc
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

namespace cb {
/**
 * Function object which returns true if lhs > rhs.
 * Equivalent to std::greater, but without having to pull in all of <functional>
 */
template <typename T>
struct greater {
    constexpr bool operator()(const T& lhs, const T& rhs) const {
        return lhs > rhs;
    }
};

/**
 * Function object which returns true if lhs >= rhs.
 * Equivalent to std::greater_equal, but without having to pull in all of
 * <functional>
 */
template <typename T>
struct greater_equal {
    constexpr bool operator()(const T& lhs, const T& rhs) const {
        return lhs >= rhs;
    }
};

/**
 * Function object which returns true if lhs < rhs.
 * Equivalent to std::less, but without having to pull in all of
 * <functional>
 */
template <typename T>
struct less {
    constexpr bool operator()(const T& lhs, const T& rhs) const {
        return lhs < rhs;
    }
};

/**
 * Function object which returns true if lhs <= rhs.
 * Equivalent to std::less_equal, but without having to pull in all of
 * <functional>
 */
template <typename T>
struct less_equal {
    constexpr bool operator()(const T& lhs, const T& rhs) const {
        return lhs <= rhs;
    }
};
} // namespace cb
