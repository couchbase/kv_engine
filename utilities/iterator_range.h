/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2021-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#pragma once

#include <stdexcept>
#include <type_traits>

namespace cb {
/**
 * Provides a simple container for a pair of iterators (or an iterator and a
 * equality-comparable sentinel) representing a range over some container.
 *
 * This impl is much simpler than boost::iterator_range [1] and has an
 * minimal interface.
 *
 * The main usage of this would be to easily support range based for loops.
 * While a class may directly implement begin() and end() to do this, if
 * the container may be iterated in multiple different fashions only one
 * can be conveniently handled in this manner.
 *
 * Such a container could expose other iteration "variants" as methods
 * returning iterator_ranges:
 *
 *  class Histogram {
 *      cb::iterator_range<...> log_view();
 *      cb::iterator_range<...> percentile_view();
 *  }
 *  ...
 *  for (const auto& value: histogram.log_view()) {...}
 *
 * Alternative solutions like `beginLog()/endLog()` and
 * `beginPercentile()/endPercentile()` quickly become clunky.
 *
 * [1]:
 * https://www.boost.org/doc/libs/1_55_0/libs/range/doc/html/range/reference/utilities/iterator_range.html
 */
template <class BeginType, class EndType = BeginType>
class iterator_range {
public:
    using Begin = std::remove_reference_t<BeginType>;
    using End = std::remove_reference_t<EndType>;

    iterator_range(BeginType begin, EndType end)
        : beginItr{std::move(begin)}, endItr{std::move(end)} {
    }

    const Begin& begin() const {
        return beginItr;
    }
    const End& end() const {
        return endItr;
    }

private:
    const Begin beginItr;
    const End endItr;
};

/**
 * Variant of iterator_range for use with non-copyable iterators.
 * Such iterators do not meet the named requirement LegacyIterator, but this
 * variant exists to support them where they currently exist.
 *
 * begin() and end() both move() the contained iterator, and shall only be
 * called once.
 */
template <class BeginType, class EndType = BeginType>
class move_only_iterator_range {
public:
    using Begin = std::remove_reference_t<BeginType>;
    using End = std::remove_reference_t<EndType>;

    move_only_iterator_range(BeginType begin, EndType end)
        : beginItr{std::move(begin)}, endItr{std::move(end)} {
    }

    Begin begin() {
        // begin shouldn't be called more than once, we move the value.
        // Could rely on use-after-move checks, but at least initially
        // an explicit check and throw is easier to validate.
        if (!beginValid) {
            throw std::logic_error(
                    "move_only_iterator_range: begin can only be called once");
        }
        beginValid = false;
        return std::move(beginItr);
    }

    End end() {
        // end shouldn't be called more than once, we move the value.
        if (!endValid) {
            throw std::logic_error(
                    "move_only_iterator_range: end can only be called once");
        }
        endValid = false;
        return std::move(endItr);
    }

private:
    Begin beginItr;
    End endItr;
    bool beginValid = true;
    bool endValid = true;
};
} // namespace cb