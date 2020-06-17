/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2019 Couchbase, Inc.
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

#include <platform/non_negative_counter.h>

#include <memory>

/**
 * Class provides a generic memory allocator that allows memory usage to
 * be tracked.  It is currently used by the CheckPointQueue.
 *
 * The allocator boilerplate was taken from the following web-site:
 * https://howardhinnant.github.io/allocator_boilerplate.html
 *
 * Example use:
 *
 * typedef std::deque<int, MemoryTrackingAllocator<int>> Deque;
 *
 *  MemoryTrackingAllocator<int> allocator;
 *  Deque theDeque(allocator);
 *
 *  To return the bytes allocated use:
 *
 *  *(theDeque.get_allocator().getBytesAllocated())
 *
 *  See /engines/ep/tests/module_tests/memory_tracking_allocator_test.cc for
 *  full code.
 *
 *  Note: A shared counter is required because it is possible to end up with
 *  multiple allocators, which need to allocate different object types.
 *  For example std::deque allocates space for the metadata and space for
 *  the items using separate allocators.  Therefore bytesAllocated needs to
 *  be a shared_ptr so that the bytes allocated by each allocator are
 *  combined together into a single value.
 *
 *  Also it is important to NOT use the default allocator of the container
 *  e.g. Deque theDeque().  Otherwise the allocator accounting will not be
 *  correct if the container rebinds, as is the case when using a std::deque
 *  container with libc++.
 *
 */

template <class T>
class MemoryTrackingAllocator {
public:
    using value_type = T;

    MemoryTrackingAllocator() noexcept
        : bytesAllocated(std::make_shared<cb::NonNegativeCounter<size_t>>(0)) {
    }

    template <class U>
    explicit MemoryTrackingAllocator(
            MemoryTrackingAllocator<U> const& other) noexcept
        /**
         * Used during a rebind and therefore need to copy over the
         * byteAllocated shared pointer.
         */
        : bytesAllocated(other.getBytesAllocated()) {
    }

    MemoryTrackingAllocator(const MemoryTrackingAllocator& other) noexcept =
            default;

    MemoryTrackingAllocator(MemoryTrackingAllocator&& other) noexcept
        // The move ctor can be invoked when the underlying container is
        // moved; however the old (moved-from) container while _logically_
        // empty could still _physically_ own allocations (e.g. a sentinal node
        // in a std::list). As such, we need to ensure the moved-from
        // container's allocator (i.e. other) can still perform deallocations,
        // hence bytesAllocated should only be copied, not moved-from.
        : bytesAllocated(other.getBytesAllocated()) {
    }

    MemoryTrackingAllocator& operator=(
            const MemoryTrackingAllocator& other) noexcept = default;

    MemoryTrackingAllocator& operator=(
            MemoryTrackingAllocator&& other) noexcept {
        // Need to copy bytesAllocated even though this is the move-assignment
        // operator - see comment in move ctor for rationale.
        bytesAllocated = other.getBytesAllocated();
        return *this;
    }

    value_type* allocate(std::size_t n) {
        *bytesAllocated += n * sizeof(T);
        return static_cast<value_type*>(::operator new(n * sizeof(value_type)));
    }

    void deallocate(value_type* p, std::size_t n) noexcept {
        *bytesAllocated -= n * sizeof(T);
        ::operator delete(p);
    }

    MemoryTrackingAllocator select_on_container_copy_construction() const {
        /**
         * We call the constructor to ensure that on a copy the new allocator
         * has its own byteAllocated counter instance.
         */
        return MemoryTrackingAllocator();
    }

    auto getBytesAllocated() const {
        return bytesAllocated;
    }

private:
    std::shared_ptr<cb::NonNegativeCounter<size_t>> bytesAllocated;
};

template <class T, class U>
bool operator==(MemoryTrackingAllocator<T> const& a,
                MemoryTrackingAllocator<U> const& b) noexcept {
    return a.getBytesAllocated() == b.getBytesAllocated();
}

template <class T, class U>
bool operator!=(MemoryTrackingAllocator<T> const& a,
                MemoryTrackingAllocator<U> const& b) noexcept {
    return !(a == b);
}
