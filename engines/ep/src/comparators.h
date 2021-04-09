/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2020-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
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
