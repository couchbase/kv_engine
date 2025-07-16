/*
 * Copyright 2017 Facebook, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * This folly header originally was:
 *
 * https://github.com/facebook/folly/blob/d4aacd24/folly/AtomicBitSet.h
 *
 * Couchbase modifications to the original:
 * 1. clang-formatted
 * 2. BlockType defaults to uint8_t as we want a compact bitfield for the
 *    primary user (KV-engine StoredValue)
 * 3. Removed private inheritance from boost::noncopyable
 *   3.1 Added explicit delete of copy/move constructor/assignment
 * 4. Removed includes of other Folly libraries.
 * 5. Added mask function with narrow_cast for narrowing warnings
 * 6. This comment block
 */

#pragma once

#include <gsl/gsl-lite.hpp>
#include <array>
#include <atomic>
#include <cassert>
#include <cstddef>
#include <limits>

namespace folly {

/**
 * An atomic bitset of fixed size (specified at compile time).
 */
template <size_t N>
class AtomicBitSet {
public:
    /**
     * Construct an AtomicBitSet; all bits are initially false.
     */
    AtomicBitSet();

    AtomicBitSet(const AtomicBitSet&) = delete;
    AtomicBitSet& operator=(const AtomicBitSet&) = delete;
    AtomicBitSet(AtomicBitSet&&) = delete;
    AtomicBitSet& operator=(AtomicBitSet&&) = delete;

    /**
     * Set bit idx to true, using the given memory order. Returns the
     * previous value of the bit.
     *
     * Note that the operation is a read-modify-write operation due to the use
     * of fetch_or.
     */
    bool set(size_t idx, std::memory_order order = std::memory_order_seq_cst);

    /**
     * Set bit idx to false, using the given memory order. Returns the
     * previous value of the bit.
     *
     * Note that the operation is a read-modify-write operation due to the use
     * of fetch_and.
     */
    bool reset(size_t idx, std::memory_order order = std::memory_order_seq_cst);

    /**
     * Set bit idx to the given value, using the given memory order. Returns
     * the previous value of the bit.
     *
     * Note that the operation is a read-modify-write operation due to the use
     * of fetch_and or fetch_or.
     *
     * Yes, this is an overload of set(), to keep as close to std::bitset's
     * interface as possible.
     */
    bool set(size_t idx,
             bool value,
             std::memory_order order = std::memory_order_seq_cst);

    /**
     * Read bit idx.
     */
    bool test(size_t idx,
              std::memory_order order = std::memory_order_seq_cst) const;

    /**
     * Same as test() with the default memory order.
     */
    bool operator[](size_t idx) const;

    /**
     * Return the size of the bitset.
     */
    constexpr size_t size() const {
        return N;
    }

private:
    // @couchbase: Force uint8_t as we want a compact bitfield
    typedef uint8_t BlockType;
    typedef std::atomic<BlockType> AtomicBlockType;

    static constexpr size_t kBitsPerBlock =
            std::numeric_limits<BlockType>::digits;

    static constexpr size_t blockIndex(size_t bit) {
        return bit / kBitsPerBlock;
    }

    static constexpr size_t bitOffset(size_t bit) {
        return bit % kBitsPerBlock;
    }

    static constexpr BlockType mask(size_t idx) {
        assert(idx < N * kBitsPerBlock);
        return gsl::narrow_cast<BlockType>(kOne << bitOffset(idx));
    }

    // avoid casts
    static constexpr BlockType kOne = 1;

    std::array<AtomicBlockType, N> data_;
};

// value-initialize to zero
template <size_t N>
inline AtomicBitSet<N>::AtomicBitSet() : data_() {
}

template <size_t N>
inline bool AtomicBitSet<N>::set(size_t idx, std::memory_order order) {
    return data_[blockIndex(idx)].fetch_or(mask(idx), order) & mask(idx);
}

template <size_t N>
inline bool AtomicBitSet<N>::reset(size_t idx, std::memory_order order) {
    return data_[blockIndex(idx)].fetch_and(~mask(idx), order) & mask(idx);
}

template <size_t N>
inline bool AtomicBitSet<N>::set(size_t idx,
                                 bool value,
                                 std::memory_order order) {
    return value ? set(idx, order) : reset(idx, order);
}

template <size_t N>
inline bool AtomicBitSet<N>::test(size_t idx, std::memory_order order) const {
    assert(idx < N * kBitsPerBlock);
    return data_[blockIndex(idx)].load(order) & mask(idx);
}

template <size_t N>
inline bool AtomicBitSet<N>::operator[](size_t idx) const {
    return test(idx);
}

} // namespace folly