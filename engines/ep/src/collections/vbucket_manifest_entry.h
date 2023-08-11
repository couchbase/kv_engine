/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2017-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#pragma once

#include "collections/collections_types.h"
#include "ep_types.h"
#include "memcached/engine_common.h"

#include <platform/n_byte_integer.h>
#include <platform/non_negative_counter.h>
#include <relaxed_atomic.h>

#include <memory>
#include <utility>

class StatCollector;

namespace Collections {
namespace VB {

/**
 * The Collections::VB::ManifestEntry stores the data a collection
 * needs from a vbucket's perspective.
 * - The ScopeID
 * - The TTL value of the collection (applies to items not the collection)
 * - The start seqno of the collection
 */
class ManifestEntry {
public:
    ManifestEntry(SingleThreadedRCPtr<VB::CollectionSharedMetaData> meta,
                  uint64_t startSeqno,
                  CanDeduplicate canDeduplicate,
                  cb::ExpiryLimit ttl,
                  Metered metered)
        : itemCount(0),
          highSeqno(startSeqno),
          persistedHighSeqno(startSeqno),
          meta(std::move(meta)),
          startSeqno(startSeqno),
          canDeduplicate(canDeduplicate),
          metered(metered) {
        setMaxTtl(ttl);
    }

    // Mark copy and move as deleted as it simplifies the lifetime of
    // CollectionSharedMetaData
    ManifestEntry(const ManifestEntry&) = delete;
    ManifestEntry& operator=(const ManifestEntry&) = delete;
    ManifestEntry(ManifestEntry&&) = delete;
    ManifestEntry& operator=(ManifestEntry&&) = delete;

    bool operator==(const ManifestEntry& other) const;
    bool operator!=(const ManifestEntry& other) const {
        return !(*this == other);
    }

    uint64_t getStartSeqno() const {
        return startSeqno;
    }

    void setStartSeqno(uint64_t seqno) {
        // start can only be set to a new, greater value
        if (seqno <= startSeqno) {
            throwException<std::invalid_argument>(
                    __FUNCTION__,
                    "cannot set startSeqno from:" + std::to_string(startSeqno) +
                            " to:" + std::to_string(seqno));
        }
        startSeqno = seqno;
    }

    ScopeID getScopeID() const {
        return meta->scope;
    }

    cb::ExpiryLimit getMaxTtl() const {
        if (maxTtl == std::numeric_limits<uint32_t>::max()) {
            return cb::NoExpiryLimit;
        }
        return std::chrono::seconds(maxTtl.load());
    }

    void setMaxTtl(cb::ExpiryLimit ttl) {
        if (ttl) {
            maxTtl.store(gsl::narrow<uint32_t>(ttl.value().count()));
        } else {
            maxTtl.store(std::numeric_limits<uint32_t>::max());
        }
    }

    std::string_view getName() const {
        return meta->name;
    }

    /// @return collection Metered state (Yes or No)
    Metered isMetered() const {
        return metered;
    }

    void setMetered(Metered m) {
        metered = m;
    }

    /// increment how many items are stored for this collection
    void incrementItemCount() const {
        itemCount++;
    }

    /// decrement how many items are stored for this collection
    void decrementItemCount() const {
        itemCount--;
    }

    /// update how many items are stored on disk for this collection
    void updateItemCount(ssize_t delta) const {
        itemCount += delta;
    }

    /// set how many items this collection has stored
    void setItemCount(uint64_t value) const {
        itemCount = value;
    }

    /// @return how many items are stored for this collection
    uint64_t getItemCount() const {
        return itemCount;
    }

    /// update the tracked total size (bytes) on disk for this collection
    void updateDiskSize(ssize_t delta) const {
        diskSize += delta;
    }

    /// update the tracked total size (bytes) on disk for this collection
    void setDiskSize(size_t value) const {
        diskSize = value;
    }

    /// @return the tracked total size (bytes) on disk for this collection
    size_t getDiskSize() const {
        return diskSize;
    }

    /// set the highest seqno (persisted or not) for this collection
    void setHighSeqno(uint64_t value) const {
        highSeqno.store(value, std::memory_order_relaxed);
    }

    /**
     * Force set the highest seqno (persisted or not) for this collection.
     * Required for warmup where we need to overwrite the value with 0. Would
     * rather reset for warmup than loosen the montonic constraint to weak.
     */
    void resetHighSeqno(uint64_t value) const {
        highSeqno.reset(value, std::memory_order_relaxed);
    }

    /// @return the highest seqno of any item in this collection
    uint64_t getHighSeqno() const {
        return highSeqno.load(std::memory_order_relaxed);
    }

    /// set the highest persisted seqno for this collection if the new value
    /// is greater than the previous one
    bool setPersistedHighSeqno(uint64_t value) const {
        if (value >= startSeqno) {
            persistedHighSeqno.store(value, std::memory_order_relaxed);
            return true;
        }
        return false;
    }

    /// reset the highest persisted seqno for this collection to the given value
    void resetPersistedHighSeqno(uint64_t value = 0) const {
        persistedHighSeqno.store(value, std::memory_order_relaxed);
    }

    /// @return the highest seqno of any persisted item in this collection
    uint64_t getPersistedHighSeqno() const {
        return persistedHighSeqno.load(std::memory_order_relaxed);
    }

    /// @return true if successfully added stats, false otherwise
    bool addStats(const std::string& cid,
                  Vbid vbid,
                  const StatCollector& collector) const;

    void incrementOpsStore() const {
        meta->incrementOpsStore();
    }
    void incrementOpsDelete() const {
        meta->incrementOpsDelete();
    }
    void incrementOpsGet() const {
        meta->incrementOpsGet();
    }

    AccumulatedStats getStatsForSummary() const {
        return {getItemCount(), getDiskSize()};
    }

    /**
     * Take the CollectionSharedMetaData from this entry (moves the data) to
     * the caller.
     */
    SingleThreadedRCPtr<CollectionSharedMetaData>&& takeMeta() {
        return std::move(meta);
    }

    /// @return Yes/No if this collection removes duplicate keys when possible
    CanDeduplicate getCanDeduplicate() const {
        return canDeduplicate;
    }

    void setCanDeduplicate(CanDeduplicate value) {
        canDeduplicate.store(value);
    }

private:
    /**
     * Return a string for use in throwException, returns:
     *   "VB::ManifestEntry::<thrower>:<error>, this:<ostream *this>"
     *
     * @param thrower a string for who is throwing, typically __FUNCTION__
     * @param error a string containing the error and useful data
     * @returns string as per description above
     */
    std::string getExceptionString(const std::string& thrower,
                                   const std::string& error) const;

    /**
     * throw exception with the following error string:
     *   "VB::ManifestEntry::<thrower>:<error>, this:<ostream *this>"
     *
     * @param thrower a string for who is throwing, typically __FUNCTION__
     * @param error a string containing the error and useful data
     * @throws exception
     */
    template <class exception>
    [[noreturn]] void throwException(const std::string& thrower,
                                     const std::string& error) const {
        throw exception(getExceptionString(thrower, error));
    }

    /**
     * The count of items in this collection
     * mutable - the VB:Manifest read/write lock protects this object and
     *           we can do stats updates as long as the read lock is held.
     *           The write lock is really for the Manifest map being changed.
     */
    mutable cb::AtomicNonNegativeCounter<uint64_t> itemCount;

    /**
     * The total size (bytes) of items in this collection on disk
     * mutable - the VB:Manifest read/write lock protects this object and
     *           we can do stats updates as long as the read lock is held.
     *           The write lock is really for the Manifest map being changed.
     */
    mutable cb::AtomicNonNegativeCounter<uint64_t> diskSize;

    /**
     * The highest seqno of any item (persisted or not) that belongs to this
     * collection.
     *
     * Will be hit by multiple front end threads and thus needs to be atomic.
     * We should ignore any attempts to set the value to a lower number as
     * this is a possible result of compare_exchange_weak returning false
     * inside AtomicMonotonic::operator=() (i.e. another thread has already
     * written a higher value and so the current write is no longer valid and
     * any failure can be ignored).
     *
     * mutable - the VB:Manifest read/write lock protects this object and
     *           we can do stats updates as long as the read lock is held.
     *           The write lock is really for the Manifest map being changed.
     */
    mutable AtomicMonotonic<uint64_t, IgnorePolicy> highSeqno;

    /**
     * The highest seqno of any item that has been/is currently being persisted.
     *
     * TSan would warn sporadically of a data race when we make a lot of stats
     * calls if this was not atomic. We will ignore any attempt to set the value
     * to something that is not higher than the current value as couchstore
     * can flush out of order.
     *
     * mutable - the VB:Manifest read/write lock protects this object and
     *           we can do stats updates as long as the read lock is held.
     *           The write lock is really for the Manifest map being changed.
     */
    mutable AtomicMonotonic<uint64_t, IgnorePolicy> persistedHighSeqno;

    /**
     * The 'static' metadata associated with this collection.
     * The VB::Manifest (ManifestEntry) does not own this data, but instead has
     * a reference to the data, which is stored inside the Manager.
     */
    SingleThreadedRCPtr<CollectionSharedMetaData> meta;

    /**
     * Start-seqno of the collection is recorded, items of the collection below
     * the start-seqno are logically deleted.
     *
     * 48-bit as that is the max seqno that KVStore can assign
     */
    cb::uint48_t startSeqno;

    /**
     * The collections canDeduplicate setting. This must be stored per vbucket
     * so that collection modification events (per vb) can be generated when
     * needed.
     */
    std::atomic<CanDeduplicate> canDeduplicate;

    /**
     * The collections metered setting. This must be stored per vbucket
     * so that collection modification events (per vb) can be generated when
     * needed.
     */
    std::atomic<Metered> metered;

    /**
     * maxTTL of the collection. Stored as uint32 and not cb::ExpiryLimit to
     * minimise the size increase to this object. The max permissible value is
     * 2147483647 (max int32) allowing us to use max uint32 to represent no TTL.
     */
    std::atomic<uint32_t> maxTtl{std::numeric_limits<uint32_t>::max()};
};

std::ostream& operator<<(std::ostream& os, const ManifestEntry& manifestEntry);

} // end namespace VB
} // end namespace Collections
