/*
 *     Copyright 2026-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#pragma once

#include <memcached/vbucket.h>

#include <atomic>
#include <cstddef>
#include <memory>
#include <stdexcept>
#include <type_traits>
#include <utility>
#include <vector>

/**
 * A map from Vbid to a std::shared_ptr<T>, used by the DcpProducer to hold
 * the StreamContainer of each vBucket.
 *
 * The key space is dense and bounded (0 to maxVbuckets - 1), so rather than
 * a hash table the map is a fixed size array with one slot per vBucket. A
 * slot is empty until the first insert for that vBucket, and once set it is
 * never changed or removed until the map is destroyed (to "remove" streams
 * the owner empties the StreamContainer in place). That allows find() and
 * iteration to be lock-free: a slot is published with a single
 * compare-exchange and the value it points to is immutable afterwards.
 *
 * Each slot holds a pointer to a heap allocated std::shared_ptr. The natural
 * slot type would be std::atomic<std::shared_ptr<T>>, but that isn't
 * supported by libc++ (and folly::atomic_shared_ptr is libstdc++ only).
 *
 * Unlike folly::AtomicHashArray (which was previously used) the map doesn't
 * use any thread local / process global state, so all of its memory is
 * allocated and freed by whichever bucket owns it.
 */
template <class T>
class DcpStreamsMap {
public:
    using Value = std::shared_ptr<T>;

    /**
     * Create a map which can hold values for vBuckets 0 to maxVbuckets - 1.
     *
     * @param maxVbuckets The maximum number of vBuckets in the Bucket
     */
    explicit DcpStreamsMap(size_t maxVbuckets) : slots(maxVbuckets) {
    }

    DcpStreamsMap(const DcpStreamsMap&) = delete;
    DcpStreamsMap& operator=(const DcpStreamsMap&) = delete;
    DcpStreamsMap(DcpStreamsMap&&) = delete;
    DcpStreamsMap& operator=(DcpStreamsMap&&) = delete;

    ~DcpStreamsMap() {
        for (auto& slot : slots) {
            // A non-null slot owns its value (see insert())
            std::unique_ptr<Value> owned(slot.load(std::memory_order_acquire));
        }
    }

    /**
     * Look up the value for the given vBucket.
     *
     * @param vbid The vBucket to look up
     * @return The value mapped for vbid, or an empty shared_ptr if there
     *         is none. The reference is valid for the lifetime of the map.
     */
    const Value& find(Vbid vbid) const {
        if (vbid.get() < slots.size()) {
            const auto* value =
                    slots[vbid.get()].load(std::memory_order_acquire);
            if (value) {
                return *value;
            }
        }
        return emptyValue;
    }

    /**
     * Insert a value for the given vBucket unless it already has one.
     *
     * @param vbid The vBucket to insert the value for
     * @param value The (non-null) value to insert
     * @return A pair of the value now mapped for vbid, and true if the given
     *         value was inserted or false if vbid already had a value (which
     *         is left unchanged)
     * @throws std::invalid_argument if value is null
     * @throws std::out_of_range if vbid is outside the capacity of the map
     */
    std::pair<const Value&, bool> insert(Vbid vbid, Value value) {
        if (!value) {
            throw std::invalid_argument(
                    "DcpStreamsMap::insert: value must not be null");
        }
        auto& slot = slots.at(vbid.get());
        Value* expected = slot.load(std::memory_order_acquire);
        if (expected) {
            return {*expected, false};
        }
        auto candidate = std::make_unique<Value>(std::move(value));
        if (slot.compare_exchange_strong(expected,
                                         candidate.get(),
                                         std::memory_order_acq_rel,
                                         std::memory_order_acquire)) {
            return {*candidate.release(), true};
        }
        return {*expected, false};
    }

    /**
     * Invoke the function for every vBucket which has a value, in vBucket
     * order.
     *
     * @param func Function object called as func(Vbid, const Value&)
     */
    template <class Func>
    void for_each(Func&& func) const {
        for (size_t idx = 0; idx < slots.size(); ++idx) {
            const auto* value = slots[idx].load(std::memory_order_acquire);
            if (value) {
                func(Vbid(static_cast<Vbid::id_type>(idx)), *value);
            }
        }
    }

    /**
     * Invoke the function for every vBucket which has a value (in vBucket
     * order) until it returns something which evaluates to true.
     *
     * @param func Function object called as func(Vbid, const Value&)
     * @return The first result of func which evaluates to true, or a value
     *         initialised result if there is none
     */
    template <class Func>
    auto find_if(Func&& func) const
            -> std::invoke_result_t<Func&, Vbid, const Value&> {
        for (size_t idx = 0; idx < slots.size(); ++idx) {
            const auto* value = slots[idx].load(std::memory_order_acquire);
            if (value) {
                auto rv = func(Vbid(static_cast<Vbid::id_type>(idx)), *value);
                if (rv) {
                    return rv;
                }
            }
        }
        return {};
    }

    /**
     * @return The number of vBuckets the map can hold values for
     */
    size_t capacity() const {
        return slots.size();
    }

private:
    const Value emptyValue{};

    /**
     * One slot per vBucket. A slot is either null or owns a non-null value
     * which is immutable until the map is destroyed.
     */
    std::vector<std::atomic<Value*>> slots;
};
