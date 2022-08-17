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

#include <folly/SharedMutex.h>

namespace cb {

/**
 * An opaque reference to either of SharedMutex::ReadHolder or
 * SharedMutex::WriteHolder.
 *
 * The intention is to provide the same level of assurance that a lock is held
 * as that achieved by passing the ReadHolder/WriteHolder by const& into
 * functions. The user of this struct needs to make sure that the proper locks
 * are used and that the lock used to initialize this object outlives this tag.
 *
 * Caveat: Cannot unlock(), upgrade/downgrade the locks. This type has the
 * semantics of a _const_ reference.
 *
 * The type argument is an optional "tag" type which can be used to distinguish
 * between `SharedLockRef` instances at the type-level.
 * (e.g. SharedLockRef<StateATag>, SharedLockRef<StateBTag>)
 */
template <typename Tag = void>
class SharedLockRef {
public:
    SharedLockRef(const folly::SharedMutex::ReadHolder& rhl) : ptr(&rhl) {
    }

    SharedLockRef(const folly::SharedMutex::WriteHolder& whl) : ptr(&whl) {
    }

    // SharedLockRefs are just opaque pointers and can be copied.
    SharedLockRef(const SharedLockRef&) = default;
    SharedLockRef& operator=(const SharedLockRef&) = default;

    SharedLockRef(SharedLockRef&&) = default;
    SharedLockRef& operator=(SharedLockRef&&) = default;

    ~SharedLockRef() noexcept {
        // "Touch" the memory of the lock object. The intention is to enable
        // ASan to detect a use-after-free of the lock object used to create
        // this instance. Reading out char does not break aliasing rules.
        (void)*reinterpret_cast<const char*>(ptr);
    }

private:
    const void* ptr;
};

} // namespace cb