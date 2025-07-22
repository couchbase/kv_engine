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

#include <gsl/gsl-lite.hpp>
#include <cctype>
#include <compare>
#include <concepts>
#include <cstdint>
#include <limits>

namespace cb::natsort {

namespace detail {

/**
 * Parses the next contiguous sequence of digits as a number.
 * Advances the iterator up to the end of the sequence.
 * Stops iteration before the number might overflow, in which case the iterator
 * be left at the first unconsumed digit.
 */
template <typename Int = std::intmax_t, typename FwdIt>
Int parseNumber(FwdIt& it, FwdIt end) {
    Expects(it != end);
    Int number = 0;
    constexpr auto overflowCheck = std::numeric_limits<Int>::max() / 10 - 1;
    for (; it != end && std::isdigit(*it) && number < overflowCheck; ++it) {
        int digit = *it - '0';
        number *= 10;
        number += digit;
    }
    return number;
}

/**
 * Compares the following sequence of digits (numerically) or non-digit
 * characters (lexicographically).
 */
template <typename FwdIt1, typename FwdIt2>
int compareAtom(FwdIt1& it1, FwdIt1 end1, FwdIt2& it2, FwdIt2 end2) {
    const auto isDigit1 = std::isdigit(*it1);
    const auto isDigit2 = std::isdigit(*it2);
    if (!isDigit1) {
        if (!isDigit2) {
            // Compare characters.
            auto ch1 = *it1;
            auto ch2 = *it2;
            ++it1;
            ++it2;
            return ch1 - ch2;
        }
        // Put numbers before other characters.
        return 1;
    }

    // Compare numbers.
    auto num1 = parseNumber(it1, end1);
    auto num2 = parseNumber(it2, end2);

    if (num1 < num2) {
        return -1;
    }
    if (num1 > num2) {
        return 1;
    }
    return 0;
}

} // namespace detail

/**
 * Compares strings in alphabetical order, except that multi-digit numbers are
 * treated atomically AKA Natural sort order.
 */
template <typename FwdIt1, typename FwdIt2>
int compare(FwdIt1 it1, FwdIt1 end1, FwdIt2 it2, FwdIt2 end2) {
    for (;;) {
        const auto empty1 = it1 == end1;
        const auto empty2 = it2 == end2;
        if (empty1) {
            if (empty2) {
                return 0;
            }
            return -1;
        }
        if (empty2) {
            return 1;
        }

        int cmp = detail::compareAtom(it1, end1, it2, end2);
        // Ensure we've not advanced past the end (when iterator supports it).
        if constexpr (std::totally_ordered<FwdIt1>) {
            Ensures(it1 <= end1);
        }
        if constexpr (std::totally_ordered<FwdIt2>) {
            Ensures(it2 <= end2);
        }

        if (cmp != 0) {
            return cmp;
        }
    }
}

struct compare_three_way {
    template <typename T, typename U>
    std::strong_ordering operator()(const T& t, const U& u) const {
        int c = compare(t.begin(), t.end(), u.begin(), u.end());
        if (c == 0) {
            return std::strong_ordering::equal;
        }
        return c < 0 ? std::strong_ordering::less
                     : std::strong_ordering::greater;
    }
};

struct less {
    template <typename T>
    bool operator()(const T& a, const T& b) const {
        return compare(a.begin(), a.end(), b.begin(), b.end()) < 0;
    }
};

struct greater {
    template <typename T>
    bool operator()(const T& a, const T& b) const {
        return compare(a.begin(), a.end(), b.begin(), b.end()) > 0;
    }
};

} // namespace cb::natsort
