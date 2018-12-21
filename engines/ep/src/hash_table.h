/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2016 Couchbase, Inc
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

#include "config.h"
#include "probabilistic_counter.h"
#include "stored-value.h"
#include "storeddockey.h"

#include <platform/histogram.h>
#include <platform/non_negative_counter.h>

#include <array>
#include <functional>

class AbstractStoredValueFactory;
class HashTableVisitor;
class HashTableDepthVisitor;

/**
 * Mutation types as returned by store commands.
 */
enum class MutationStatus : uint16_t {
    NotFound, //!< The item was not found for update
    InvalidCas, //!< The wrong CAS identifier was sent for a CAS update
    WasClean, //!< The item was clean before this mutation
    WasDirty, //!< This item was already dirty before this mutation
    IsLocked, //!< The item is locked and can't be updated.
    NoMem, //!< Insufficient memory to store this item.
    NeedBgFetch, //!< Require a bg fetch to process SET op,
    IsPendingSyncWrite, //!< The item a pending SyncWrite and can't be updated.
};
std::string to_string(MutationStatus status);

/**
 * Result from add operation.
 */
enum class AddStatus : uint16_t {
    Success, //!< Add was successful.
    NoMem, //!< No memory for operation
    Exists, //!< Did not update -- item exists with this key
    UnDel, //!< Undeletes an existing dirty item
    AddTmpAndBgFetch, //!< Create a tmp item and schedule a bg metadata fetch
    BgFetch //!< Schedule a bg metadata fetch to process ADD op
};

/**
 * Result from temporary add operation.
 */
enum class TempAddStatus : uint8_t {
    NoMem,  //No memory for operation
    BgFetch //Schedule a background fetch
};

/**
 * A container of StoredValue instances.
 *
 * The HashTable class is an unordered, associative array which maps
 * StoredDocKeys to StoredValues.
 *
 * Overview
 * ========
 *
 * It supports a limited degree of concurrent access - the underlying
 * HashTable buckets are guarded by N ht_locks; where N is typically of the
 * order of the number of CPUs. Essentially ht bucket B is guarded by
 * mutex B mod N.
 *
 * StoredValue objects can have their value (Blob object) ejected, making the
 * value non-resident. Such StoredValues are still in the HashTable, and their
 * metadata (CAS, revSeqno, bySeqno, etc) is still accessible, but the value
 * cannot be accessed until it is fetched from disk. This is value eviction.
 *
 * Additionally, we can eject the whole StoredValue from the HashTable.
 * We no longer know if the item even exists from the HashTable, and need to go
 * to disk to definitively know if it exists or not. Note that even though the
 * HashTable has no direct knowledge of the item now, it /does/ track the total
 * number of items which exist but are not resident (see numNonResidentItems).
 * This is full eviction.
 *
 * The HashTable can hold StoredValues which are either 'valid' (i.e. represent
 * the current state of that key), or which are logically deleted. Typically
 * StoredValues which are deleted are only held in the HashTable for a short
 * period of time - until the deletion is recorded on disk by the Flusher, at
 * which point they are removed from the HashTable by PersistenceCallback (we
 * don't want to unnecessarily spend memory on items which have been deleted).
 *
 * The HashTable can hold up to 1 Committed Item, *and* up to 1 Pending Item
 * for each Key.
 * Committed items are the "normal" item stored - they represent the current
 * value of a key.
 * Pending items are needed to support SyncWrites - they are items which are
 * proposed to be the "next" value for a key, but are awaiting confirmation
 * from sufficient other nodes before they can be committed - at which point
 * the Pending Item is converted to a Committed one.
 *
 * Different uses of the HashTable will want to access potentially different
 * items:
 *   - when performing a read() then only Committed items will typically want
 *     to be searched for - even if a Pending item exists it should be ignored.
 *   - When performing a write() then both Committed and Pending items typically
 *     want to be searched - as a Pending item will typically block any changes
 *     to the key until it is later committed (or aborted).
 *
 * To facilitate this, find() operations on the HashTable expose two
 * _perspectives_ which items can be searched via:
 *
 * A) findForRead() - uses Committed perspective. Only returns Committed
 *    items; Pending items are ignored.
 *
 * B) findForWrite() - used Pending perspective. Returns both Committed
 *    and Pending items; if both Pending and Committed items exist for a
 *    key then the Pending item is returned.
 *
 * Implementation
 * ==============
 *
 * The HashTable object is implemented as a vector of buckets; each bucket
 * being unique_ptr<StoredValue>. Keys are hashed mod size() to select the
 * bucket; then chaining is used (StoredValue::chain_next_or_replacement) to
 * handle any collisions.
 *
 * The HashTable can be resized if it grows too full - this is done by
 * acquiring all the ht_locks, and then allocating a new vector of buckets and
 * re-hashing all elements into the new table. While resizing is occuring all
 * other access to the HashTable is blocked.
 *
 * Support for holding both Committed and Pending items requires that we
 * can represent having for each key, either:
 *  1. No item present
 *  2. Only a Pending item
 *  3. Only a Committed item
 *  4. Both Pending and Committed items.
 *
 * This is implemented by using the same key for both (so they map to the same
 * hash bucket), and then connecting them via the chain_next_of_replacement
 * field. If both Pending or Committed items are present then the Pending item
 * is the first one in the chain; the StoredValue::committed flag is used to
 * distinguish between them.
 */
class HashTable {
public:
    /**
     * Datatype counts; one element for each combination of datatypes
     * (e.g. JSON, JSON+XATTR, JSON+Snappy, etc...)
     */
    using DatatypeCombo = std::array<cb::NonNegativeCounter<size_t>,
                                     mcbp::datatype::highest + 1>;

    /// Under what perspective (view) should the HashTable be accessed.
    enum class Perspective {
        /// Only access items which are Committed
        Committed,
        /// Access both Committed and Pending items.
        Pending,
    };

    /**
     * Represents a position within the hashtable.
     *
     * Currently opaque (and constant), clients can pass them around but
     * cannot reposition the iterator.
     */
    class Position {
    public:
        // Allow default construction positioned at the start,
        // but nothing else.
        Position() : ht_size(0), lock(0), hash_bucket(0) {}

        bool operator==(const Position& other) const {
            return (ht_size == other.ht_size) &&
                   (lock == other.lock) &&
                   (hash_bucket == other.hash_bucket);
        }

        bool operator!=(const Position& other) const {
            return ! (*this == other);
        }

    private:
        Position(size_t ht_size_, int lock_, int hash_bucket_)
          : ht_size(ht_size_),
            lock(lock_),
            hash_bucket(hash_bucket_) {}

        // Size of the hashtable when the position was created.
        size_t ht_size;
        // Lock ID we are up to.
        size_t lock;
        // hash bucket ID (under the given lock) we are up to.
        size_t hash_bucket;

        friend class HashTable;
        friend std::ostream& operator<<(std::ostream& os, const Position& pos);
    };

    /**
     * Records various statistics about a HashTable object.
     *
     * Due to the number of items we typically store in HashTable, it isn't
     * feasible to calculate statistics on-demand - e.g. to count the number
     * of deleted items we don't want to iterate the HashTable counting how
     * many SVs have isDeleted() set.
     * Instead, we maintain a number of 'running' counters, updating the overall
     * counts whenever items are added/removed/their state changes.
     *
     * Clients can read current statistics values via the various get() methods,
     * however updating statistics values is performed by the prologue() and
     * epilogue() methods.
     */
    class Statistics {
    public:
        Statistics(EPStats& epStats) : epStats(epStats) {
        }

        /**
         * Set of properties on a StoredValue which are considerd by statistics
         * tracking.
         * Used by prologue() / epilogue() to update HashTable statistics.
         */
        struct StoredValueProperties {
            StoredValueProperties(const StoredValue* sv);

            // Following members are set to the equivilent property of the
            // // given StoredValue.
            int size = 0;
            int metaDataSize = 0;
            int uncompressedSize = 0;
            protocol_binary_datatype_t datatype = PROTOCOL_BINARY_RAW_BYTES;
            bool isValid = false;
            bool isResident = false;
            bool isDeleted = false;
            bool isTempItem = false;
            bool isSystemItem = false;
        };

        /**
         * Snapshot the "pre" StoredValue statistics-related properties before
         * modifying a StoredValue.
         *
         * This function should be called before modifying *any* StoredValue
         * object, if modifying it may affect any of the HashTable counts. For
         * example, when we remove a StoredValue we must decrement any counts
         * which it matches; or when we change its datatype we must decrement
         * the count of the old datatype and increment the new count.
         *
         * It returns a StoredValueProperties object which records all
         * StoredValue properties which affect HashTable statistics. For example
         * it records the value size, datatype, temp flag, ...
         *
         * After modifying the StoredValue, the StoredValueProperties object
         * should be passed into epilogue() which compares the "post" state of
         * the StoredValue with the "pre" properties and updates statistics as
         * appropriate.
         *
         * See also: epilogue().
         * @param sv StoredValue which is about to be modified.
         */
        StoredValueProperties prologue(const StoredValue* sv) const;

        /**
         * Update HashTable statistics after modifying a StoredValue.
         *
         * This function should be called after modifying *any* StoredValue
         * object, if modifying it may affect any of the HashTable counts. For
         * example, if the datatype of a StoredValue may have changed; then
         * datatypeCounts needs to be updated.
         *
         * @param pre StoredValueProperties from before the StoredValue
         *                   was modified.
         * @param post StoredValue which has just been modified.
         */
        void epilogue(StoredValueProperties pre, const StoredValue* post);

        /// Reset the values of all statistics to zero.
        void reset();

        size_t getNumItems() const {
            return numItems;
        }

        size_t getNumNonResidentItems() const {
            return numNonResidentItems;
        }

        size_t getNumDeletedItems() const {
            return numDeletedItems;
        }

        size_t getNumTempItems() const {
            return numTempItems;
        }

        size_t getNumSystemItems() const {
            return numSystemItems;
        }

        DatatypeCombo getDatatypeCounts() const {
            return datatypeCounts;
        }

        size_t getCacheSize() const {
            return cacheSize;
        }

        size_t getMetaDataMemory() const {
            return metaDataMemory;
        }

        size_t getMemSize() const {
            return memSize;
        }

        size_t getUncompressedMemSize() const {
            return uncompressedMemSize;
        }

    private:
        /// Count of alive & deleted, in-memory non-resident and resident items.
        /// Excludes temporary items.
        cb::NonNegativeCounter<size_t> numItems;

        /// Count of alive, non-resident items.
        cb::NonNegativeCounter<size_t> numNonResidentItems;

        /// Count of deleted items.
        cb::NonNegativeCounter<size_t> numDeletedItems;

        /// Count of items where StoredValue::isTempItem() is true.
        cb::NonNegativeCounter<size_t> numTempItems;

        /// Count of items where StoredValue resides in system namespace
        cb::NonNegativeCounter<size_t> numSystemItems;

        /**
         * Number of documents of a given datatype. Includes alive
         * (non-deleted), documents in the HashTable.
         * For value eviction includes resident & non-resident items (as the
         * datatype is part of the metadata), for full-eviction will only
         * include resident items.
         */
        DatatypeCombo datatypeCounts = {};

        //! Cache size (fixed-length fields in StoredValue + keylen + valuelen).
        std::atomic<size_t> cacheSize = {};

        //! Meta-data size (fixed-length fields in StoredValue + keylen).
        std::atomic<size_t> metaDataMemory = {};

        //! Memory consumed by items in this hashtable.
        std::atomic<size_t> memSize = {};

        /// Memory consumed if the items were uncompressed.
        std::atomic<size_t> uncompressedMemSize = {};

        EPStats& epStats;
    };

    /**
     * Represents a locked hash bucket that provides RAII semantics for the lock
     *
     * A simple container which holds a lock and the bucket_num of the
     * hashtable bucket it has the lock for.
     */
    class HashBucketLock {
    public:
        HashBucketLock()
            : bucketNum(-1) {}

        HashBucketLock(int bucketNum, std::mutex& mutex)
            : bucketNum(bucketNum), htLock(mutex) {
        }

        HashBucketLock(HashBucketLock&& other)
            : bucketNum(other.bucketNum), htLock(std::move(other.htLock)) {
        }

        HashBucketLock(const HashBucketLock& other) = delete;

        int getBucketNum() const {
            return bucketNum;
        }

        const std::unique_lock<std::mutex>& getHTLock() const {
            return htLock;
        }

        std::unique_lock<std::mutex>& getHTLock() {
            return htLock;
        }

    private:
        int bucketNum;
        std::unique_lock<std::mutex> htLock;
    };

    /**
     * Create a HashTable.
     *
     * @param st the global stats reference
     * @param svFactory Factory to use for constructing stored values
     * @param initialSize the number of hash table buckets to initially create.
     * @param locks the number of locks in the hash table
     */
    HashTable(EPStats& st,
              std::unique_ptr<AbstractStoredValueFactory> svFactory,
              size_t initialSize,
              size_t locks);

    ~HashTable();

    size_t memorySize() {
        return sizeof(HashTable)
            + (size * sizeof(StoredValue*))
            + (mutexes.size() * sizeof(std::mutex));
    }

    /**
     * Get the number of hash table buckets this hash table has.
     */
    size_t getSize(void) { return size; }

    /**
     * Get the number of locks in this hash table.
     */
    size_t getNumLocks(void) { return mutexes.size(); }

    /**
     * Get the number of in-memory non-resident and resident items within
     * this hash table.
     * - Includes items which are marked as deleted (but held in memory).
     * - Does *not* include temporary items - as such the actual number of
     *   HashTable slots occupied is the sum of getNumInMemoryItems() and
     *   getNumTempItems()
     */
    size_t getNumInMemoryItems() const {
        return valueStats.getNumItems();
    }

    /**
     * Get the number of deleted items in the hash table.
     */
    size_t getNumDeletedItems() const {
        return valueStats.getNumDeletedItems();
    }

    /**
     * Get the number of in-memory non-resident items within this hash table.
     */
    size_t getNumInMemoryNonResItems() const {
        return valueStats.getNumNonResidentItems();
    }

    /**
     * Get the number of non-resident and resident items managed by
     * this hash table. Includes items marked as deleted.
     */
    size_t getNumItems() const {
        return valueStats.getNumItems();
    }

    size_t getNumSystemItems() const {
        return valueStats.getNumSystemItems();
    }

    /**
     * Sets the frequencyCounterSaturated function to the callback function
     * passed in.
     * @param - callbackFunction  The function to set
     *                            frequencyCounterSaturated to.
     */
    void setFreqSaturatedCallback(std::function<void()> callbackFunction) {
        frequencyCounterSaturated = callbackFunction;
    }

    /**
     * Gets a reference to the frequencyCounterSaturated function.
     * Currently used for testing purposes.
     *
     */
    std::function<void()>& getFreqSaturatedCallback() {
        return frequencyCounterSaturated;
    }

    /**
     * Remove in case of a temporary item
     *
     * @param hbl Hash table lock
     * @param v   Stored Value that needs to be cleaned up
     *
     */
    void cleanupIfTemporaryItem(const HashBucketLock& hbl,
                                StoredValue& v);

    /**
     * Get the number of items whose values are ejected from this hash table.
     */
    size_t getNumEjects(void) { return numEjects; }

    /**
     * Get the total cache size of this hash table.
     *
     * Defined as: (StoredValue + keylen + valuelen) for all items in HT.
     */
    size_t getCacheSize() const {
        return valueStats.getCacheSize();
    }

    /**
     * Get the total item memory size in this hash table.
     */
    size_t getItemMemory() const {
        return valueStats.getMemSize();
    }

    /**
     * Get the total metadata memory size in this hash table.
     */
    size_t getMetadataMemory() const {
        return valueStats.getMetaDataMemory();
    }

    size_t getUncompressedItemMemory() const {
        return valueStats.getUncompressedMemSize();
    }

    /**
     * Clear the hash table.
     *
     * @param deactivate true when this hash table is being destroyed completely
     */
    void clear(bool deactivate = false);

    /**
     * Get the number of times this hash table has been resized.
     */
    size_t getNumResizes() { return numResizes; }

    /**
     * Get the number of temp. items within this hash table.
     */
    size_t getNumTempItems() const {
        return valueStats.getNumTempItems();
    }

    DatatypeCombo getDatatypeCounts() const {
        return valueStats.getDatatypeCounts();
    }

    /**
     * Automatically resize to fit the current data.
     */
    void resize();

    /**
     * Resize to the specified size.
     */
    void resize(size_t to);

    /**
     * Result of the findForRead() method.
     */
    struct FindROResult {
        /// If find successful then pointer to found StoredValue; else nullptr.
        const StoredValue* storedValue;
        /**
         * The (locked) HashBucketLock for the given key. Note this always
         * returns a locked object; even if the requested key doesn't exist.
         * This is to facilitate use-cases where the caller subsequently needs
         * to insert a StoredValue for this key, to avoid unlocking and
         * re-locking the mutex.
         */
        HashBucketLock lock;
    };

    /**
     * Find an item with the specified key for read-only access.
     *
     * Uses the Committed perspective so only StoredValues which are committed
     * will be returned.
     *
     * @param key The key of the item to find
     * @param trackReference Should this lookup update referenced status (i.e.
     *                       increase the hotness of this key?)
     * @param wantsDeleted whether a deleted value needs to be returned
     *                     or not
     * @return A FindROResult consisting of:
     *         - a pointer to a StoredValue -- NULL if not found
     *         - a (locked) HashBucketLock for the key's hash bucket.
     */
    FindROResult findForRead(
            const DocKey& key,
            TrackReference trackReference = TrackReference::Yes,
            WantsDeleted wantsDeleted = WantsDeleted::No);

    /**
     * Result of the find() methods.
     */
    struct FindResult {
        /// If find successful then pointer to found StoredValue; else nullptr.
        StoredValue* storedValue;
        /**
         * The (locked) HashBucketLock for the given key. Note this always
         * returns a locked object; even if the requested key doesn't exist.
         * This is to facilitate use-cases where the caller subsequently needs
         * to insert a StoredValue for this key, to avoid unlocking and
         * re-locking the mutex.
         */
        HashBucketLock lock;
    };

    /**
     * Find an item with the specified key for write access.
     *
     * Does not modify referenced status, as typically the lookup of a SV to
     * change shouldn't affect the (old) reference count; the reference count
     * will get updated later part of actually changing the item.
     *
     * Uses the Pending perspective so StoredValues which are either committed
     * or pending will be returned.
     *
     * @param key The key of the item to find
     * @param wantsDeleted whether a deleted value should be returned
     *                     or not. Defaults to Yes as when writing one would
     *                     typically overwrite an existing deleted item.
     * @return A FindResult consisting of:
     *         - a pointer to a StoredValue -- NULL if not found
     *         - a (locked) HashBucketLock for the key's hash bucket. Note
     *         this always returns a locked object; even if the requested key
     *           doesn't exist. This is to facilitate use-cases where the caller
     *         subsequently needs to insert a StoredValue for this key, to
     *         avoid unlocking and re-locking the mutex.
     */
    FindResult findForWrite(const DocKey& key,
                            WantsDeleted wantsDeleted = WantsDeleted::Yes);

    /**
     * Find a resident item
     *
     * @param rnd a randomization input
     * @return an item -- NULL if not fount
     */
    std::unique_ptr<Item> getRandomKey(long rnd);

    /**
     * Set an Item into the this hashtable
     *
     * @param val the Item to store
     *
     * @return a result indicating the status of the store
     */
    MutationStatus set(Item& val);

    /**
     * Commit a pending SyncWrite; marking it at committed and removing any
     * existing Committed item with the same key.
     *
     * @param hbl Hash table bucket lock that must be held.
     * @param v Reference to the StoredValue to be committed.
     */
    void commit(const HashBucketLock& hbl, StoredValue& v);

    /**
     * Store the given compressed buffer as a value in the
     * given StoredValue
     *
     * @param buf buffer holding compressed data
     * @param v   StoredValue in which compressed data has
     *            to be stored
     */
    void storeCompressedBuffer(cb::const_char_buffer buf, StoredValue& v);

    /**
     * Result of an Update operation.
     */
    struct UpdateResult {
        /// Status of the operation.
        MutationStatus status;
        // If the update was successful (WasClean or WasDirty); points to the
        // updated value; otherwise nullptr.
        StoredValue* storedValue;
    };

    /**
     * Updates an existing StoredValue in the HT.
     * Assumes that HT bucket lock is grabbed.
     *
     * @param hbl Hash table bucket lock that must be held.
     * @param[in] v Reference to the StoredValue to be updated.
     * @param itm Item to be updated.
     *
     * @return Result of the operation; containing the status and pointer
     *         to updated storedValue (if successful).
     *
     * Note: while this is logically an update operation, if the Item is
     *       Pending and the existing StoredValue is Committed then we create a
     *       second physical StoredValue object, so v should be treated as an
     *       [in] param only.
     */
    UpdateResult unlocked_updateStoredValue(const HashBucketLock& hbl,
                                            StoredValue& v,
                                            const Item& itm);

    /**
     * Adds a new StoredValue in the HT.
     * Assumes that HT bucket lock is grabbed.
     *
     * @param hbl Hash table bucket lock that must be held.
     * @param itm Item to be added.
     *
     * @return Ptr of the StoredValue added. This function always succeeds and
     *         returns non-null ptr
     */
    StoredValue* unlocked_addNewStoredValue(const HashBucketLock& hbl,
                                            const Item& itm);

    /**
     * Replaces a StoredValue in the HT with its copy, and releases the
     * ownership of the StoredValue.
     * We need this when we want to update (or soft delete) the StoredValue
     * without an item for the update (or soft delete) and still keep around
     * stale StoredValue.
     * Assumes that HT bucket lock is grabbed.
     *
     * @param hbl Hash table bucket lock that must be held.
     * @param vToCopy StoredValue to be replaced by its copy.
     *
     * @return Ptr of the copy of the StoredValue added. This is owned by the
     *         hash table.
     *         UniquePtr to the StoredValue replaced by its copy. This is NOT
     *         owned by the hash table anymore.
     */
    std::pair<StoredValue*, StoredValue::UniquePtr> unlocked_replaceByCopy(
            const HashBucketLock& hbl, const StoredValue& vToCopy);
    /**
     * Logically (soft) delete the item in ht
     * Assumes that HT bucket lock is grabbed.
     * Also assumes that v is in the hash table.
     *
     * @param htLock Hash table lock that must be held
     * @param v Reference to the StoredValue to be soft deleted
     * @param onlyMarkDeleted indicates if we must reset the StoredValue or
     *                        just mark deleted
     * @param delSource The source of the deletion (explicit or expiry)
     */
    void unlocked_softDelete(const std::unique_lock<std::mutex>& htLock,
                             StoredValue& v,
                             bool onlyMarkDeleted,
                             DeleteSource delSource);

    /**
     * Find an item within a specific bucket assuming you already
     * locked the bucket.
     *
     * @param key the key of the item to find
     * @param bucket_num the bucket number
     * @param wantsDeleted true if soft deleted items should be returned
     * @param perspective Under what perspective should the HashTable be
     *        searched
     * @return a pointer to a StoredValue -- NULL if not found
     */
    StoredValue* unlocked_find(
            const DocKey& key,
            int bucket_num,
            WantsDeleted wantsDeleted,
            TrackReference trackReference,
            Perspective perspective = Perspective::Committed);

    /**
     * Get a lock holder holding a lock for the given bucket
     *
     * @param bucket the bucket number to lock
     * @return HashBucektLock which contains a lock and the hash bucket number
     */
    inline HashBucketLock getLockedBucket(int bucket) {
        return HashBucketLock(bucket, mutexes[mutexForBucket(bucket)]);
    }

    /**
     * Get a lock holder holding a lock for the bucket for the given
     * hash.
     *
     * @param h the input hash
     * @return HashBucketLock which contains a lock and the hash bucket number
     */
    inline HashBucketLock getLockedBucketForHash(int h) {
        while (true) {
            if (!isActive()) {
                throw std::logic_error("HashTable::getLockedBucket: "
                        "Cannot call on a non-active object");
            }
            int bucket = getBucketForHash(h);
            HashBucketLock rv(bucket, mutexes[mutexForBucket(bucket)]);
            if (bucket == getBucketForHash(h)) {
                return rv;
            }
        }
    }

    /**
     * Get a lock holder holding a lock for the bucket for the hash of
     * the given key.
     *
     * @param s the key
     * @return HashBucektLock which contains a lock and the hash bucket number
     */
    inline HashBucketLock getLockedBucket(const DocKey& key) {
        if (!isActive()) {
            throw std::logic_error("HashTable::getLockedBucket: Cannot call on a "
                    "non-active object");
        }
        return getLockedBucketForHash(key.hash());
    }

    /**
     * Delete a key from the cache without trying to lock the cache first
     * (Please note that you <b>MUST</b> acquire the mutex before calling
     * this function!!!
     *
     * @param hbl HashBucketLock that must be held
     * @param key the key to delete
     */
    void unlocked_del(const HashBucketLock& hbl, const DocKey& key);

    /**
     * Visit all items within this hashtable.
     */
    void visit(HashTableVisitor &visitor);

    /**
     * Visit all items within this call with a depth visitor.
     */
    void visitDepth(HashTableDepthVisitor &visitor);

    /**
     * Visit the items in this hashtable, starting the iteration from the
     * given startPosition and allowing the visit to be paused at any point.
     *
     * During visitation, the visitor object can request that the visit
     * is stopped after the current item. The position passed to the
     * visitor can then be used to restart visiting at the *APPROXIMATE*
     * same position as it paused.
     * This is approximate as hashtable locks are released when the
     * function returns, so any changes to the hashtable may cause the
     * visiting to restart at the slightly different place.
     *
     * As a consequence, *DO NOT USE THIS METHOD* if you need to guarantee
     * that all items are visited!
     *
     * @param visitor The visitor object to use.
     * @param start_pos At what position to start in the hashtable.
     * @return The final HashTable position visited; equal to
     *         HashTable::end() if all items were visited otherwise the
     *         position to resume from.
     */
    Position pauseResumeVisit(HashTableVisitor& visitor, Position& start_pos);

    /**
     * Return a position at the end of the hashtable. Has similar semantics
     * as STL end() (i.e. one past the last element).
     */
    Position endPosition() const;

    /**
     * Get the max deleted revision seqno seen so far.
     */
    uint64_t getMaxDeletedRevSeqno() const {
        return maxDeletedRevSeqno.load();
    }

    /**
     * Set the max deleted seqno (required during warmup).
     */
    void setMaxDeletedRevSeqno(const uint64_t seqno) {
        maxDeletedRevSeqno.store(seqno);
    }

    /**
     * Update maxDeletedRevSeqno to a (possibly) new value.
     */
    void updateMaxDeletedRevSeqno(const uint64_t seqno) {
        atomic_setIfBigger(maxDeletedRevSeqno, seqno);
    }

    /**
     * Eject an item meta data and value from memory.
     * @param hbl Lock for this item.
     * @param vptr the reference to the pointer to the StoredValue instance.
     *             This is passed as a reference as it may be modified by this
     *             function (see note below).
     * @param policy item eviction policy
     * @return true if an item is ejected.
     *
     * NOTE: Upon a successful ejection (and if full eviction is enabled)
     *       the StoredValue will be deleted, therefore it is *not* safe to
     *       access vptr after calling this function if it returned true.
     */
    bool unlocked_ejectItem(const HashTable::HashBucketLock& hbl,
                            StoredValue*& vptr,
                            item_eviction_policy_t policy);

    /**
     * Restore the value for the item.
     * Assumes that HT bucket lock is grabbed.
     *
     * @param htLock Hash table lock that must be held
     * @param itm the item to be restored
     * @param v corresponding StoredValue
     *
     * @return true if restored; else false
     */
    bool unlocked_restoreValue(const std::unique_lock<std::mutex>& htLock,
                               const Item& itm,
                               StoredValue& v);

    /**
     * Restore the metadata of of a temporary item upon completion of a
     * background fetch.
     * Assumes that HT bucket lock is grabbed.
     *
     * @param htLock Hash table lock that must be held
     * @param itm the Item whose metadata is being restored
     * @param v corresponding StoredValue
     */
    void unlocked_restoreMeta(const std::unique_lock<std::mutex>& htLock,
                              const Item& itm,
                              StoredValue& v);

    /**
     * Releases an item(StoredValue) in the hash table, but does not delete it.
     * It will pass out the removed item to the caller who can decide whether to
     * delete it or not.
     * Once removed the item will not be found if we look in the HT later, even
     * if the item is not deleted. Hence the deletion of this
     * removed StoredValue must be handled by another (maybe calling) module.
     * Assumes that the hash bucket lock is already held.
     *
     * @param hbl HashBucketLock that must be held
     * @param key the key to delete
     *
     * @return the StoredValue that is removed from the HT
     */
    StoredValue::UniquePtr unlocked_release(const HashBucketLock& hbl,
                                                  const DocKey& key);

    /**
     * Insert an item during Warmup into the HashTable.
     * TODO: Consider splitting into the different primitive operations
     * occuring,
     * and exposing those methods individually. It is likely that will reduce
     * duplication with other HashTable functions.
     *
     * @param itm Item to insert
     * @param eject Should the item be immediately ejected?
     * @param keyMetaDataOnly Is the item being inserted metadata-only?
     * @param evictionPolicy What eviction policy should be used if eject is
     * true?
     */
    MutationStatus insertFromWarmup(Item& itm,
                                    bool eject,
                                    bool keyMetaDataOnly,
                                    item_eviction_policy_t evictionPolicy);

    /**
     * Dump a representation of the HashTable to stderr.
     */
    void dump() const;

private:
    // The container for actually holding the StoredValues.
    using table_type = std::vector<StoredValue::UniquePtr>;

    friend class StoredValue;
    friend std::ostream& operator<<(std::ostream& os, const HashTable& ht);

    inline bool isActive() const { return activeState; }
    inline void setActiveState(bool newv) { activeState = newv; }

    /**
     * Find the item with the given key.
     *
     * Helper method for findForRead / findForWrite.
     *
     * If the item exists, returns a pointer to the item along with a lock
     * on the items' hash bucket - the StoreValue can be safely accessed as
     * long as the lock object remains in scope.
     *
     * @param key the key to find
     * @param trackReference whether to track the reference or not
     * @param wantsDeleted whether a deleted value needs to be returned
     *                     or not
     * @param perspective Under what perspective should the HashTable be
     *        searched
     * @return A FindResult consisting of:
     *         - a pointer to a StoredValue -- NULL if not found
     *         - a HashBucketLock for the hash bucket of the found key. If
     * not found then HashBucketLock is empty.
     */
    FindResult find(const DocKey& key,
                    TrackReference trackReference,
                    WantsDeleted wantsDeleted,
                    Perspective perspective = Perspective::Committed);

    // The initial (and minimum) size of the HashTable.
    const size_t initialSize;

    // The size of the hash table (number of buckets) - i.e. number of elements
    // in `values`
    std::atomic<size_t> size;
    table_type values;
    std::vector<std::mutex> mutexes;
    EPStats&             stats;
    std::unique_ptr<AbstractStoredValueFactory> valFact;
    std::atomic<size_t>       visitors;

    Statistics valueStats;

    std::atomic<size_t> numEjects;
    std::atomic<size_t>       numResizes;

    std::atomic<uint64_t> maxDeletedRevSeqno;
    bool                 activeState;

    // Used by generateFreqCounter to provide an incrementing value for the
    // frequency counter of storedValues.  The frequency counter is used to
    // identify which hash table entries should be evicted first.
    ProbabilisticCounter<uint8_t> probabilisticCounter;

    // Used to hold the function to invoke when a storedValue's frequency
    // counter becomes saturated.
    // It is initialised to a function that does nothing.  This ensure
    // that if we call the function it will not cause an exception to
    // be raised.  However this is a "safety net" because when creating a
    // vbucket either via KVBucket::setVBucketState_UNLOCKED or
    // Warmup::createVBuckets we should set the function to the task
    // responsible for waking the ItemFreqDecayer task.
    std::function<void()> frequencyCounterSaturated{[]() {}};

    int getBucketForHash(int h) {
        return abs(h % static_cast<int>(size));
    }

    inline size_t mutexForBucket(size_t bucket_num) {
        if (!isActive()) {
            throw std::logic_error("HashTable::mutexForBucket: Cannot call on a "
                    "non-active object");
        }
        return bucket_num % mutexes.size();
    }

    std::unique_ptr<Item> getRandomKeyFromSlot(int slot);

    /** Searches for the first element in the specified hashChain which matches
     * predicate p, and unlinks it from the chain.
     *
     * @param chain Linked list of StoredValues to scan.
     * @param p Predicate to test each element against.
     *          The signature of the predicate function should be equivalent
     *          to the following:
     *               bool pred(const StoredValue* a);
     *
     * @return The removed element, or NULL if no matching element was found.
     */
    template <typename Pred>
    StoredValue::UniquePtr hashChainRemoveFirst(StoredValue::UniquePtr& chain,
                                                Pred p) {
        if (p(chain.get().get())) {
            // Head element:
            auto removed = std::move(chain);
            chain = std::move(removed->getNext());
            return removed;
        }

        // Not head element, start searching.
        for (StoredValue::UniquePtr* curr = &chain; curr->get()->getNext();
             curr = &curr->get()->getNext()) {
            if (p(curr->get()->getNext().get().get())) {
                // next element matches predicate - splice it out of the list.
                auto removed = std::move(curr->get()->getNext());
                curr->get()->setNext(std::move(removed->getNext()));
                return removed;
            }
        }

        // No match found.
        return nullptr;
    }

    void clear_UNLOCKED(bool deactivate);

    /**
     * Generates a new value that is either the same or higher than the input
     * value.  It is intended to be used to increment the frequency counter of a
     * storedValue.
     * @param value  The value counter to try to generate an increment for.
     * @returns      The new value that is the same or higher than value.
     */
    uint8_t generateFreqValue(uint8_t value);

    /**
     * Update the frequency counter of a given stored value.
     * @param v  reference to a value in the hash table whose frequency counter
     *           is to be updated.
     */
    void updateFreqCounter(StoredValue& v);

    DISALLOW_COPY_AND_ASSIGN(HashTable);
};

std::ostream& operator<<(std::ostream& os, const HashTable& ht);

/**
 * Base class for visiting a hash table.
 */
class HashTableVisitor {
public:
    virtual ~HashTableVisitor() {}

    /**
     * Visit an individual item within a hash table. Note that the item is
     * locked while visited (the appropriate hashTable lock is held).
     *
     * @param v a pointer to a value in the hash table
     * @return true if visiting should continue, false if it should terminate.
     */
    virtual bool visit(const HashTable::HashBucketLock& lh, StoredValue& v) = 0;

    /**
     * Function called before we visit the elements of a HashBucket. Allows
     * the derived HashTableVisitors to perform some action before iterating
     * on the HashBucket by overriding this function. For example, getting a
     * collections read lock before iterating on some HashBucket.
     */
    virtual void setUpHashBucketVisit() {
        return;
    };

    /**
     * Function called after we visit the elements of a HashBucket. Lets the
     * derived HashTableVisitors cleanup any state that may have been set
     * before iterating on some HashBucket by overriding this function.
     */
    virtual void tearDownHashBucketVisit() {
        return;
    };
};

/**
 * Hash table visitor that reports the depth of each hashtable bucket.
 */
class HashTableDepthVisitor {
public:
    virtual ~HashTableDepthVisitor() {}

    /**
     * Called once for each hashtable bucket with its depth.
     *
     * @param bucket the index of the hashtable bucket
     * @param depth the number of entries in this hashtable bucket
     * @param mem counted memory used by this hash table
     */
    virtual void visit(int bucket, int depth, size_t mem) = 0;
};

/**
 * Hash table visitor that finds the min and max bucket depths.
 */
class HashTableDepthStatVisitor : public HashTableDepthVisitor {
public:

    HashTableDepthStatVisitor()
        : depthHisto(GrowingWidthGenerator<unsigned int>(1, 1, 1.3), 10),
          size(0),
          memUsed(0),
          min(-1),
          max(0) {}

    void visit(int bucket, int depth, size_t mem) {
        (void)bucket;
        // -1 is a special case for min.  If there's a value other than
        // -1, we prefer that.
        min = std::min(min == -1 ? depth : min, depth);
        max = std::max(max, depth);
        depthHisto.add(depth);
        size += depth;
        memUsed += mem;
    }

    Histogram<unsigned int> depthHisto;
    size_t                  size;
    size_t                  memUsed;
    int                     min;
    int                     max;
};

/**
 * Track the current number of hashtable visitors.
 *
 * This class is a pretty generic counter holder that increments on
 * entry and decrements on return providing RAII guarantees around an
 * atomic counter.
 */
class VisitorTracker {
public:

    /**
     * Mark a visitor as visiting.
     *
     * @param c the counter that should be incremented (and later
     * decremented).
     */
    explicit VisitorTracker(std::atomic<size_t> *c) : counter(c) {
        counter->fetch_add(1);
    }
    ~VisitorTracker() {
        counter->fetch_sub(1);
    }
private:
    std::atomic<size_t> *counter;
};
