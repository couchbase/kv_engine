/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2021 Couchbase, Inc
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
