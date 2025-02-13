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

#include <folly/Synchronized.h>
#include <gsl/gsl-lite.hpp>
#include <atomic>
#include <mutex>

/**
 * Holds a unique_ptr<T> under a mutex and allows the underlying pointer to be
 * read with atomic semantics.
 *
 * Useful when we want to lazily initialise an object, which itself is
 * thread-safe. In that case, we want to synchronise around the initialisation /
 * other operations, but once initialised, thread-safe members of T can be
 * accessed without synchronisation.
 */
template <typename T>
class SynchronizedInitOncePtr {
public:
    SynchronizedInitOncePtr() = default;

    /**
     * Performs an operation under a mutex.
     */
    template <typename Function>
    auto withLock(Function&& function) {
        auto locked = sync.lock();
        auto finally =
                gsl::finally([this, oldPointer = locked->get(), &locked]() {
                    T* newPointer = locked->get();
                    // We can only init the object once.
                    Ensures(!oldPointer || oldPointer == newPointer);
                    // Publish changes into the std::atomic.
                    atomic.store(newPointer, std::memory_order_release);
                });
        return function(*locked);
    }

    // Const overload cannot change the unique_ptr.
    template <typename Function>
    auto withLock(Function&& function) const {
        return function(*sync.lock());
    }

    /**
     * Returns the value set at the exit of withLock.
     */
    T* getUnlocked() const {
        // Needs acquire to see all writes in the thread which set the atomic.
        return atomic.load(std::memory_order_acquire);
    }

private:
    folly::Synchronized<std::unique_ptr<T>, std::mutex> sync{nullptr};
    std::atomic<T*> atomic{nullptr};
};
