/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2019-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#pragma once

#include <iterator>
#include <string>

/**
 * Class provides checkpoint iterator functionality.
 *
 * The functionality is such that if the iterator points to a nullptr in the
 * container then the iterator skips past it (either moving forward when in
 * the ++ operator; or moving backwards when in the -- operator.
 */

template <typename C>
class CheckpointIterator {
public:
    enum class Position { begin, end };

    // The following type aliases are required to allow the iterator
    // to behave like a STL iterator so functions such as std::next work.
    using iterator_category = std::bidirectional_iterator_tag;
    using difference_type = typename C::difference_type;
    using value_type = typename C::value_type;
    using size_type = typename C::size_type;
    using pointer = typename C::pointer;
    using reference = typename C::reference;

    // these are not a STL required typedefs
    using underlying_iterator = typename C::iterator;
    using const_underlying_iterator = typename C::const_iterator;

    CheckpointIterator(std::reference_wrapper<C> c, Position p) : container(c) {
        if (p == Position::begin) {
            iter = container.get().begin();
        } else if (p == Position::end) {
            iter = container.get().end();
        } else {
            throw std::invalid_argument(
                    "CheckpointIterator - Position is invalid. "
                    "Should be either begin or end");
        }

        // Skip past any null entries, which refer to items that have been
        // de-duplicated away.
        while (!isAtEnd() && isNullElement()) {
            moveForward();
        }
    }

    auto operator++() {
        moveForward();

        // Skip past any null entries, which refer to items that have been
        // de-duplicated away.
        while (!isAtEnd() && isNullElement()) {
            moveForward();
        }
        return *this;
    }

    auto operator++(int) {
        auto beforeInc = *this;
        operator++();
        return beforeInc;
    }

    auto operator--() {
        moveBackward();

        // Skip past any null entries, which refer to items that have been
        // de-duplicated away.
        while (!isAtStart() && isNullElement()) {
            moveBackward();
        }
        return *this;
    }

    auto operator--(int) {
        auto beforeDec = *this;
        operator--();
        return beforeDec;
    }

    auto operator==(CheckpointIterator ci) {
        return (ci.iter == iter && &(ci.container.get()) == &(container.get()));
    }

    auto operator!=(CheckpointIterator ci) {
        return !operator==(ci);
    }

    auto& operator*() {
        if (isAtEnd()) {
            throw std::out_of_range(
                    "CheckpointIterator *() "
                    "index is pointing to 'end'");
        }
        return getElement();
    }

    const auto& operator*() const {
        if (isAtEnd()) {
            throw std::out_of_range(
                    "CheckpointIterator *() const "
                    "index is pointing to 'end'");
        }
        return getElement();
    }

    auto& operator-> () {
        if (isAtEnd()) {
            throw std::out_of_range(
                    "CheckpointIterator ->() "
                    "index is pointing to 'end'");
        }
        return iter;
    }

    const auto& operator-> () const {
        if (isAtEnd()) {
            throw std::out_of_range(
                    "CheckpointIterator ->() const "
                    "index is pointing to 'end'");
        }
        return iter;
    }

    /// The following is required to allow erase to be invoked on
    /// CheckpointQueue as the erase method takes a const_iter.
    explicit operator const_underlying_iterator() const {
        return iter;
    }

    auto getUnderlyingIterator() const {
        return iter;
    }

private:
    /// Is the iterator currently pointing to the "end" element.
    bool isAtEnd() const {
        return (iter == container.get().end());
    }

    /// Is the iterator currently pointing to the first element,
    bool isAtStart() const {
        return (iter == container.get().begin());
    }

    /// Is the iterator currently pointing to a null element.
    bool isNullElement() const {
        return ((*iter).get() == nullptr);
    }

    /// Get the element currently being pointed to by the iterator.
    auto& getElement() const {
        return *iter;
    }

    /// Move the iterator forwards.
    void moveForward() {
        ++iter;
    }

    /// Move the iterator backwards.
    void moveBackward() {
        --iter;
    }

    /// reference_wrapper of the container being iterated over.
    std::reference_wrapper<C> container;
    /// The Container's standard iterator
    underlying_iterator iter;
};
