/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2022 Couchbase, Inc
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

#include "vbucket_fwd.h"
#include <gsl/gsl-lite.hpp>
#include <mutex>

/**
 * Represents a locked VBucket that provides RAII semantics for the lock.
 *
 * Behaves like the underlying shared_ptr - i.e. `operator->` is overloaded
 * to return the underlying VBucket.
 */
class LockedVBucketPtr {
public:
    LockedVBucketPtr() = default;

    LockedVBucketPtr(VBucketPtr vb, std::unique_lock<std::mutex>&& lock)
        : vb(std::move(vb)), lock(std::move(lock)) {
    }

    VBucket& operator*() const {
        Expects(vb && "Attempt to dereference a null LockedVBucketPtr");
        return *vb;
    }

    VBucket* operator->() const {
        return vb.get();
    }

    explicit operator bool() const {
        return vb.operator bool();
    }

    VBucketPtr& getVB() {
        return vb;
    }

    /// Return true if this object owns the mutex.
    bool owns_lock() const {
        return lock.owns_lock();
    }

    /// Get the underlying lock (to allow caller to release the lock and
    /// reacquire it at a later time)
    std::unique_lock<std::mutex>& getLock() {
        return lock;
    }

private:
    VBucketPtr vb;
    std::unique_lock<std::mutex> lock;
};
