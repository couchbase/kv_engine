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

#if (defined(__x86_64__) || defined(_M_X64) || defined(__s390x__) || \
     defined(__aarch64__))

#define CB_HAS_PACKED_PTR 1

#include <platform/n_byte_integer.h>

/**
 * A 6-byte pointer type.
 *
 * On some platforms (like x86_64, AArch64), the virtual address space is 48-bit
 * wide, and so the 16 high order bits in a pointer value are unused.
 *
 * Note that some compilers (observed in GCC 10 to 12) will emit less efficient
 * code for reads and writes to value of this type.
 *
 * Also see TaggedPtr, which has the same size and alignment requirements as a
 * void*, but allows the 16 high order bits to be set/cleared separately from
 * the pointer value.
 */
template <typename T>
class PackedPtr {
public:
    PackedPtr(T* t = nullptr) noexcept : ptr(reinterpret_cast<uint64_t>(t)) {
    }

    PackedPtr& operator=(T* t) noexcept {
        ptr = reinterpret_cast<uint64_t>(t);
        return *this;
    }

    T* operator->() const noexcept {
        return *this;
    }

    T& operator*() const noexcept {
        return *static_cast<T*>(*this);
    }

    operator T*() const noexcept {
        return reinterpret_cast<T*>(static_cast<uint64_t>(ptr));
    }

private:
    cb::uint48_t ptr;
};

static_assert(sizeof(PackedPtr<int>) == 6);

/**
 * A deleter implementation for PackedPtrs.
 */
template <typename T, typename Deleter>
struct PackedPtrDeleter {
    using pointer = PackedPtr<T>;

    void operator()(pointer p) noexcept(noexcept(Deleter()(p))) {
        Deleter()(p);
    }
};

#endif // supported platforms
