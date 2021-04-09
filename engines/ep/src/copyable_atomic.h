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

#include <atomic>

/**
 * A drop-in replacement for std::atomic<T>, when you want to be able to
 * copy (or assign) the atomic type (std::atomic<> is not copyable or
 * copy-assignable).
 *
 * Useful when an atomic variable is a member of a struct/class and you
 * want to allow the owning struct to be copied / assigned to.
 *
 * Note: By the C++ definition this class isn't strictly "atomic", as a copy /
 *       copy-assign must perform a load from one (atomic) object and a store
 *       to another object.
 */
template <class T>
class CopyableAtomic : public std::atomic<T> {
public:
    CopyableAtomic() noexcept = default;

    constexpr CopyableAtomic(const CopyableAtomic& other) noexcept
        : std::atomic<T>(other.load()) {
    }

    constexpr CopyableAtomic(CopyableAtomic&& other) noexcept
        : std::atomic<T>(other.load()) {
    }

    constexpr CopyableAtomic& operator=(const CopyableAtomic& val) noexcept {
        this->store(val.load());
        return *this;
    }

    constexpr CopyableAtomic& operator=(CopyableAtomic&& val) noexcept {
        this->store(val.load());
        return *this;
    }
};
