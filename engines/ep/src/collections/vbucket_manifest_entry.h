/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2017 Couchbase, Inc
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

#include "collections/collections_types.h"
#include "memcached/engine_common.h"
#include "stored-value.h"
#include "systemevent.h"

#include <platform/non_negative_counter.h>
#include <platform/sized_buffer.h>

#include <memory>

namespace Collections {
namespace VB {

/**
 * The Collections::VB::ManifestEntry stores the data a collection
 * needs from a vbucket's perspective.
 * - The ScopeID
 * - The TTL value of the collection (applies to items not the collection)
 * - The seqno lifespace of the collection
 */
class ManifestEntry {
public:
    ManifestEntry(ScopeID scopeID,
                  cb::ExpiryLimit maxTtl,
                  int64_t startSeqno,
                  int64_t endSeqno)
        : startSeqno(-1),
          endSeqno(-1),
          scopeID(scopeID),
          maxTtl(maxTtl),
          diskCount(0),
          highSeqno(0),
          persistedHighSeqno(0) {
        // Setters validate the start/end range is valid
        setStartSeqno(startSeqno);
        setEndSeqno(endSeqno);
    }

    /**
     * Explicitly define the copy constructor otherwise it would be
     * implicitly deleted via the deleted copy constructor in std::atomic
     * (which is used inside highSeqno - AtomicMonotonic). This is required for
     * using ManifestEntries in an unordered_map.
     */
    ManifestEntry(const ManifestEntry& other);
    ManifestEntry& operator=(const ManifestEntry& other);

    bool operator==(const ManifestEntry& other) const;

    int64_t getStartSeqno() const {
        return startSeqno;
    }

    void setStartSeqno(int64_t seqno) {
        // Enforcing that start/end are not the same, they should always be
        // separated because they represent start/end mutations.
        if (seqno < 0 || seqno <= startSeqno || seqno == endSeqno) {
            throwException<std::invalid_argument>(
                    __FUNCTION__,
                    "cannot set startSeqno to " + std::to_string(seqno));
        }
        startSeqno = seqno;
    }

    int64_t getEndSeqno() const {
        return endSeqno;
    }

    void setEndSeqno(int64_t seqno) {
        // Enforcing that start/end are not the same, they should always be
        // separated because they represent start/end mutations.
        if (seqno != StoredValue::state_collection_open &&
            (seqno <= endSeqno || seqno == startSeqno)) {
            throwException<std::invalid_argument>(
                    __FUNCTION__,
                    "cannot set "
                    "endSeqno to " +
                            std::to_string(seqno));
        }
        endSeqno = seqno;
    }

    void resetEndSeqno() {
        endSeqno = StoredValue::state_collection_open;
    }

    ScopeID getScopeID() const {
        return scopeID;
    }

    cb::ExpiryLimit getMaxTtl() const {
        return maxTtl;
    }

    /**
     * A collection is open when the start is greater than the end.
     * An open collection is one that is readable and writable by the data
     * path.
     */
    bool isOpen() const {
        return startSeqno > endSeqno;
    }

    /**
     * A collection is being deleted when the endSeqno is not the special
     * state_collection_open value.
     */
    bool isDeleting() const {
        return endSeqno != StoredValue::state_collection_open;
    }

    /// increment how many items are stored on disk for this collection
    void incrementDiskCount() const {
        diskCount++;
    }

    /// decrement how many items are stored on disk for this collection
    void decrementDiskCount() const {
        diskCount--;
    }

    /// set how many items this collection has stored
    void setDiskCount(uint64_t value) {
        diskCount = value;
    }

    /// @return how many items are stored on disk for this collection
    uint64_t getDiskCount() const {
        return diskCount;
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
    void setPersistedHighSeqno(uint64_t value) const {
        if (value > persistedHighSeqno) {
            persistedHighSeqno = value;
        }
    }

    /// reset the highest persisted seqno for this collection to the given value
    void resetPersistedHighSeqno(uint64_t value = 0) const {
        persistedHighSeqno = value;
    }

    /// @return the highest seqno of any persisted item in this collection
    uint64_t getPersistedHighSeqno() const {
        return persistedHighSeqno;
    }

    /// @return true if successfully added stats, false otherwise
    bool addStats(const std::string& cid,
                  Vbid vbid,
                  const void* cookie,
                  const AddStatFn& add_stat) const;

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
     * Collection life-time is recorded as the seqno the collection was added
     * to the seqno of the point we started to delete it.
     *
     * If a collection is not being deleted then endSeqno has a value of
     * StoredValue::state_collection_open to indicate this.
     */
    int64_t startSeqno;
    int64_t endSeqno;

    /// The scope the collection belongs to
    ScopeID scopeID;

    /// The max_ttl of the collection
    cb::ExpiryLimit maxTtl;

    /**
     * The count of items in this collection
     * mutable - the VB:Manifest read/write lock protects this object and
     *           we can do stats updates as long as the read lock is held.
     *           The write lock is really for the Manifest map being changed.
     */
    mutable cb::NonNegativeCounter<uint64_t, cb::ThrowExceptionUnderflowPolicy>
            diskCount;

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
     * Not Monotonic as couchstore does not necessarily call the saveDocs
     * callback on items in the same order that we queued them.
     *
     * mutable - the VB:Manifest read/write lock protects this object and
     *           we can do stats updates as long as the read lock is held.
     *           The write lock is really for the Manifest map being changed.
     */
    mutable uint64_t persistedHighSeqno;
};

std::ostream& operator<<(std::ostream& os, const ManifestEntry& manifestEntry);

} // end namespace VB
} // end namespace Collections
