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

#include <memory>
#include <mutex>

/**
 * This is a simplified implementation of std::atomic_shared_ptr
 * (https://en.cppreference.com/w/cpp/memory/shared_ptr/atomic2)
 *
 * It provides a shared_ptr like object which allows thread-safe access to the
 * underlying pointed-to object, by associating a mutex with a shared_ptr.
 *
 * It provides *no* synchronization of the underlying pointed-to object - that
 * object needs to mediate any access by multiple threads itself.
 *
 * Example use would be a single shared_ptr -like object which is accessed by
 * multiple threads; this is racy with a normal shared_ptr object (The standard
 * only guarantees thread-safety if each thread has it's own instance of a
 * shared_ptr object which it accesses the pointed-to object via). This class
 * avoids the race by acquiring the associated mutex around any access.
 *
 * Note this adds a relatively large space cost compared to a plain shared_ptr -
 * shared_ptr is 16 Bytes on x86-64 Linux but AtomicSharedPtr is 56 Bytes.
 * As such this may not be suitable where a large number of shared_ptr-like
 * objects are needed and space is a concern.
 *
 * @todo: This can be replaced with std::atomic_shared_ptr once we get to C++20,
 * or equivalent (e.g. folly/AtomicSharedPtr.h) when available.
 */
namespace cb {
template <class T>
class AtomicSharedPtr {
public:
    explicit AtomicSharedPtr(std::shared_ptr<T> r) noexcept
        : ptr(std::move(r)) {
    }

    AtomicSharedPtr<T>& operator=(std::shared_ptr<T>&& r) {
        std::lock_guard<std::mutex> lock(mutex);
        ptr = r;
        return *this;
    }

    std::shared_ptr<T> load() const {
        std::lock_guard<std::mutex> lock(mutex);
        return ptr;
    }

    void reset() {
        // We want to perform the reset() of the underlying shared_ptr /not/
        // under lock, for two reasons:
        // 1. To minimise the scope of the locked region (performance)
        // 2. If the reset() would reduce the ref-count to zero (and hence call
        //    the dtor of the managed object), then we don't want to hold the
        //    mutex as that could introduce lock ordering issues.
        // Therefore move from the ptr under lock; then call reset() on the
        // temporary to avoid holding the lock when reset() is called.
        std::shared_ptr<T> temp;
        {
            std::lock_guard<std::mutex> lock(mutex);
            temp = std::move(ptr);
        }
        temp.reset();
    }

    std::shared_ptr<T> operator->() const noexcept {
        return load();
    }

    explicit operator bool() const noexcept {
        return bool(operator->());
    }

private:
    mutable std::mutex mutex;
    std::shared_ptr<T> ptr;
};

} // namespace cb
