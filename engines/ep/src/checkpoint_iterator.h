/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2019 Couchbase, Inc
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
#include <iterator>
#include <string>

/**
 * Class provides checkpoint iterator functionality.  It assumes the
 * container is index based (i.e. deque or vector).
 *
 * The functionality is such that if the iterator points to a nullptr in the
 * container then the iterator skips past it (either moving forward when in
 * the ++ operator; or moving backwards when in the -- operator.
 */

template <typename C>
class CheckpointIterator {
public:
    // The following type aliases are required to allow the iterator
    // to behave like a STL iterator so functions such as std::next work.
    using iterator_category = std::bidirectional_iterator_tag;
    using difference_type = typename C::difference_type;
    using value_type = typename C::value_type;
    using size_type = typename C::size_type;
    using pointer = typename C::pointer;
    using reference = typename C::reference;

    CheckpointIterator(std::reference_wrapper<C> c, size_type i)
        : container(c), index(i) {
        if (i == 0) {
            iter = container.get().begin();
        } else {
            iter = container.get().end();
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

    auto operator--() {
        moveBackward();

        // Skip past any null entries, which refer to items that have been
        // de-duplicated away.
        while (!isAtStart() && isNullElement()) {
            moveBackward();
        }
        return *this;
    }

    auto operator==(CheckpointIterator ci) {
        return (ci.index == index && ci.container.get() == container.get());
    }

    auto operator!=(CheckpointIterator ci) {
        return !operator==(ci);
    }

    auto& operator*() {
        if (isAtEnd()) {
            throw std::out_of_range(
                    "CheckpointIterator *() index is "
                    "pointing to 'end'");
        }
        return getElement();
    }

    auto& operator*() const {
        if (isAtEnd()) {
            throw std::out_of_range(
                    "CheckpointIterator *() const "
                    "index is pointing to 'end'");
        }
        return getElement();
    }

private:
    /// Is the iterator currently pointing to the "end" element.
    bool isAtEnd() const {
        // The end index is equal to the container size.
        auto end = container.get().size();
        return (index == end);
    }

    /// Is the iterator currently pointing to the first element,
    bool isAtStart() const {
        return (index == 0);
    }

    /// Is the iterator currently pointing to a null element.
    bool isNullElement() const {
        return ((container.get())[index].get() == nullptr);
        // return ((*iter).get() == nullptr);
    }

    /// Get the element currently being pointed to by the iterator.
    auto& getElement() const {
        return (container.get())[index];
        // return *iter;
    }

    /// Move the iterator forwards.
    void moveForward() {
        ++index;
        ++iter;
    }

    /// Move the iterator backwards.
    void moveBackward() {
        --index;
        --iter;
    }

    /// Reference to container to iterate over.  We are using a
    // reference_wrapper so we can perform a copy.
    std::reference_wrapper<C> container;
    /// Index into the container.
    size_type index;
    /// The Container's standard iterator
    typename C::iterator iter;
};
