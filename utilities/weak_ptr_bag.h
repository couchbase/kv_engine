/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2022-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#pragma once

#include <folly/Synchronized.h>
#include <algorithm>
#include <memory>
#include <mutex>
#include <vector>

/**
 * A thread-safe unordered collection of weakly-referenced objects.
 *
 * The contained list of weak pointers is compacted automatically.
 */
template <typename T, typename Mutex = std::mutex>
class WeakPtrBag {
public:
    /**
     * Adds an item into the bag. The item will be weakly-referenced, so
     * it might not be returned by a call to getItems if its use count reached
     * zero before that.
     */
    void push(std::shared_ptr<T> ptr) {
        items.withLock([ptr = std::move(ptr)](auto& locked) {
            compact(locked);
            locked.push_back(ptr);
        });
    }

    /**
     * Returns strong references to the items in the bag which have a non-zero
     * use count.
     */
    std::vector<std::shared_ptr<T>> getNonExpired() const {
        // Compact the list of tasks and return the once which are alive.
        return items.withLock([](auto& locked) {
            compact(locked);

            std::vector<std::shared_ptr<T>> alive;
            alive.reserve(locked.size());

            for (auto&& weak : locked) {
                if (auto strong = weak.lock(); strong) {
                    alive.push_back(std::move(strong));
                }
            }

            return alive;
        });
    }

    /**
     * Compacts the list of weak pointers by removing the expired weak pointers
     * from it.
     *
     * Similarly to vector::shrink_to_fit, this method might release the unused
     * capacity.
     *
     * @returns The number of removed weak pointers.
     */
    size_t compact() {
        return items.withLock([](auto& locked) {
            auto initialSize = locked.size();
            compact(locked);
            // Manual compact() -- there might be a reason why it was called
            // manually. Ask the implementation to consider reducing the
            // capacity of the vector.
            locked.shrink_to_fit();
            return initialSize - locked.size();
        });
    }

private:
    static void compact(std::vector<std::weak_ptr<T>>& ptrs) {
        ptrs.erase(std::partition(ptrs.begin(),
                                  ptrs.end(),
                                  [](auto& ptr) { return !ptr.expired(); }),
                   ptrs.end());
    }

    // The list of weak_ptrs. Mutable because we want to compact the list on
    // access.
    mutable folly::Synchronized<std::vector<std::weak_ptr<T>>, Mutex> items;
};
