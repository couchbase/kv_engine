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

#include "stored-value.h"

class HashTableStatVisitor;
class HashTableVisitor;
class HashTableDepthVisitor;
class PauseResumeHashTableVisitor;

/**
 * A container of StoredValue instances.
 *
 * The HashTable class is an unordered, associative array which maps keys
 * (a binary std::string) to StoredValue.
 *
 * It supports a limited degreee of concurrent access - the underlying
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
 */
class HashTable {
public:

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
     * Create a HashTable.
     *
     * @param st the global stats reference
     * @param s the number of hash table buckets
     * @param l the number of locks in the hash table
     */
    HashTable(EPStats &st, size_t s = 0, size_t l = 0);

    ~HashTable();

    size_t memorySize() {
        return sizeof(HashTable)
            + (size * sizeof(StoredValue*))
            + (n_locks * sizeof(std::mutex));
    }

    /**
     * Get the number of hash table buckets this hash table has.
     */
    size_t getSize(void) { return size; }

    /**
     * Get the number of locks in this hash table.
     */
    size_t getNumLocks(void) { return n_locks; }

    /**
     * Get the number of in-memory non-resident and resident items within
     * this hash table.
     */
    size_t getNumInMemoryItems(void) { return numItems; }

    /**
     * Get the number of in-memory non-resident items within this hash table.
     */
    size_t getNumInMemoryNonResItems(void) { return numNonResidentItems; }

    /**
     * Get the number of non-resident and resident items managed by
     * this hash table. Note that this will be equal to getNumItems() if
     * VALUE_ONLY_EVICTION is chosen as a cache management.
     */
    size_t getNumItems(void) {
        return numTotalItems;
    }

    void setNumTotalItems(size_t totalItems) {
        numTotalItems = totalItems;
    }

    void decrNumItems(void) {
        size_t count;
        do {
            count = numItems.load();
            if (count == 0) {
                LOG(EXTENSION_LOG_DEBUG,
                    "Cannot decrement numItems, value at 0 already");
                break;
            }
        } while (!numItems.compare_exchange_strong(count, count - 1));
    }

    void decrNumTotalItems(void) {
        size_t count;
        do {
            count = numTotalItems.load();
            if (count == 0) {
                LOG(EXTENSION_LOG_DEBUG,
                    "Cannot decrement numTotalItems, value at 0 already");
                break;
            }
        } while (!numTotalItems.compare_exchange_strong(count, count - 1));
    }

    void decrNumNonResidentItems(void) {
        size_t count;
        do {
            count = numNonResidentItems.load();
            if (count == 0) {
                LOG(EXTENSION_LOG_DEBUG,
                    "Cannot decrement numNonResidentItems, value at 0 already");
                break;
            }
        } while (!numNonResidentItems.compare_exchange_strong(count, count - 1));
    }

    /**
     * Get the number of items whose values are ejected from this hash table.
     */
    size_t getNumEjects(void) { return numEjects; }

    /**
     * Get the total item memory size in this hash table.
     */
    size_t getItemMemory(void) { return memSize; }

    /**
     * Clear the hash table.
     *
     * @param deactivate true when this hash table is being destroyed completely
     *
     * @return a stat visitor reporting how much stuff was removed
     */
    HashTableStatVisitor clear(bool deactivate = false);

    /**
     * Get the number of times this hash table has been resized.
     */
    size_t getNumResizes() { return numResizes; }

    /**
     * Get the number of temp. items within this hash table.
     */
    size_t getNumTempItems(void) { return numTempItems; }

    /**
     * Automatically resize to fit the current data.
     */
    void resize();

    /**
     * Resize to the specified size.
     */
    void resize(size_t to);

    /**
     * Find the item with the given key.
     *
     * @param key the key to find
     * @return a pointer to a StoredValue -- NULL if not found
     */
    StoredValue *find(const std::string &key, bool trackReference=true);

    /**
     * Find a resident item
     *
     * @param rnd a randomization input
     * @return an item -- NULL if not fount
     */
    Item *getRandomKey(long rnd);

    /**
     * Set a new Item into this hashtable. Use this function when your item
     * doesn't contain meta data.
     *
     * @param val the Item to store
     * @param policy item eviction policy
     * @return a result indicating the status of the store
     */
    mutation_type_t set(Item &val,
                        item_eviction_policy_t policy = VALUE_ONLY)
    {
        return set(val, val.getCas(), true, false, policy);
    }

    /**
     * Set an Item into the this hashtable. Use this function to do a set
     * when your item includes meta data.
     *
     * @param val the Item to store
     * @param cas This is the cas value for the item <b>in</b> the cache
     * @param allowExisting should we allow existing items or not
     * @param hasMetaData should we keep the same revision seqno or increment it
     * @param policy item eviction policy
     * @param isReplication true if issued by consumer (for replication)
     * @return a result indicating the status of the store
     */
    mutation_type_t set(Item &val, uint64_t cas, bool allowExisting,
                        bool hasMetaData = true,
                        item_eviction_policy_t policy = VALUE_ONLY);

    mutation_type_t unlocked_set(StoredValue*& v, Item &val, uint64_t cas,
                                 bool allowExisting, bool hasMetaData = true,
                                 item_eviction_policy_t policy = VALUE_ONLY,
                                 bool maybeKeyExists=true,
                                 bool isReplication = false);

    /**
     * Insert an item to this hashtable. This is called from the backfill
     * so we need a bit more logic here. If we're trying to insert a partial
     * item we don't allow the object to be stored there (and if you try to
     * insert a full item we're only allowing an item without the value
     * in memory...)
     *
     * @param val the Item to insert
     * @param policy item eviction policy
     * @param eject true if we should eject the value immediately
     * @param partial is this a complete item, or just the key and meta-data
     * @return a result indicating the status of the store
     */
    mutation_type_t insert(Item &itm, item_eviction_policy_t policy,
                           bool eject, bool partial);

    /**
     * Add an item to the hash table iff it doesn't already exist.
     *
     * @param val the item to store
     * @param policy item eviction policy
     * @param isDirty true if the item should be marked dirty on store
     * @param storeVal true if the value should be stored (paged-in)
     * @return an indication of what happened
     */
    add_type_t add(Item &val, item_eviction_policy_t policy,
                   bool isDirty = true);

    /**
     * Unlocked version of the add() method.
     *
     * @param bucket_num the locked partition where the key belongs
     * @param v the stored value to do this operation on
     * @param val the item to store. On success it will have some fields
     *            updated (e.g. sequence numbers).
     * @param policy item eviction policy
     * @param isDirty true if the item should be marked dirty on store
     * @param isReplication true if issued by consumer (for replication)
     * @return an indication of what happened
     */
    add_type_t unlocked_add(int &bucket_num,
                            StoredValue*& v,
                            Item &val,
                            item_eviction_policy_t policy,
                            bool isDirty,
                            bool maybeKeyExists,
                            bool isReplication);

    /**
     * Add a temporary item to the hash table iff it doesn't already exist.
     *
     * NOTE: This method should be called after acquiring the correct
     *       bucket/partition lock.
     *
     * @param bucket_num the locked partition where the key belongs
     * @param key the key for which a temporary item needs to be added
     * @param policy item eviction policy
     * @param isReplication true if issued by consumer (for replication)
     * @return an indication of what happened
     */
    add_type_t unlocked_addTempItem(int &bucket_num,
                                    const const_char_buffer key,
                                    item_eviction_policy_t policy,
                                    bool isReplication = false);

    /**
     * Mark the given record logically deleted.
     *
     * @param key the key of the item to delete
     * @param cas the expected CAS of the item (or 0 to override)
     * @param policy item eviction policy
     * @return an indicator of what the deletion did
     */
    mutation_type_t softDelete(const const_char_buffer key, uint64_t cas,
                               item_eviction_policy_t policy = VALUE_ONLY);

    mutation_type_t unlocked_softDelete(StoredValue *v,
                                        uint64_t cas,
                                        item_eviction_policy_t policy = VALUE_ONLY);

    /**
     * Unlocked implementation of softDelete.
     */
    mutation_type_t unlocked_softDelete(StoredValue *v,
                                        uint64_t cas,
                                        ItemMetaData &metadata,
                                        item_eviction_policy_t policy,
                                        bool use_meta=false);

    /**
     * Find an item within a specific bucket assuming you already
     * locked the bucket.
     *
     * @param key the key of the item to find
     * @param bucket_num the bucket number
     * @param wantsDeleted true if soft deleted items should be returned
     *
     * @return a pointer to a StoredValue -- NULL if not found
     */
    StoredValue* unlocked_find(const const_char_buffer key, int bucket_num,
                               bool wantsDeleted=false,
                               bool trackReference=true);

    /**
     * Compute a hash for the given string.
     *
     * @param str the beginning of the string
     * @param len the number of bytes in the string
     *
     * @return the hash value
     */
    inline int hash(const char *str, const size_t len) {
        if (!isActive()) {
            throw std::logic_error("HashTable::hash: Cannot call on a "
                    "non-active object");
        }
        int h=5381;

        for(size_t i=0; i < len; i++) {
            h = ((h << 5) + h) ^ str[i];
        }

        return h;
    }

    /**
     * Compute a hash for the given string.
     *
     * @param s the string
     * @return the hash value
     */
    inline int hash(const std::string &s) {
        return hash(s.data(), s.length());
    }

    /**
     * Get a lock holder holding a lock for the given bucket
     *
     * @param bucket the bucket to lock
     * @return a locked LockHolder
     */
    inline LockHolder getLockedBucket(int bucket) {
        LockHolder rv(mutexes[mutexForBucket(bucket)]);
        return rv;
    }

    /**
     * Get a lock holder holding a lock for the bucket for the given
     * hash.
     *
     * @param h the input hash
     * @param bucket output parameter to receive a bucket
     * @return a locked LockHolder
     */
    inline LockHolder getLockedBucket(int h, int *bucket) {
        while (true) {
            if (!isActive()) {
                throw std::logic_error("HashTable::getLockedBucket: "
                        "Cannot call on a non-active object");
            }
            *bucket = getBucketForHash(h);
            LockHolder rv(mutexes[mutexForBucket(*bucket)]);
            if (*bucket == getBucketForHash(h)) {
                return rv;
            }
        }
    }

    /**
     * Get a lock holder holding a lock for the bucket for the hash of
     * the given key.
     *
     * @param s the start of the key
     * @param n the size of the key
     * @param bucket output parameter to receive a bucket
     * @return a locked LockHolder
     */
    inline LockHolder getLockedBucket(const char *s, size_t n, int *bucket) {
        return getLockedBucket(hash(s, n), bucket);
    }

    /**
     * Get a lock holder holding a lock for the bucket for the hash of
     * the given key.
     *
     * @param s the key
     * @param bucket output parameter to receive a bucket
     * @return a locked LockHolder
     */
    inline LockHolder getLockedBucket(const std::string &s, int *bucket) {
        return getLockedBucket(hash(s.data(), s.size()), bucket);
    }

    inline LockHolder getLockedBucket(const const_char_buffer key, int *bucket) {
        return getLockedBucket(hash(key.data(), key.size()), bucket);
    }

    /**
     * Delete a key from the cache without trying to lock the cache first
     * (Please note that you <b>MUST</b> acquire the mutex before calling
     * this function!!!
     *
     * @param key the key to delete
     * @param bucket_num the bucket to look in (must already be locked)
     * @return true if an object was deleted, false otherwise
     */
    bool unlocked_del(const const_char_buffer key, int bucket_num);

    /**
     * Delete the item with the given key.
     *
     * @param key the key to delete
     * @return true if the item existed before this call
     */
    bool del(const const_char_buffer key) {
        int bucket_num(0);
        LockHolder lh = getLockedBucket(key, &bucket_num);
        return unlocked_del(key, bucket_num);
    }

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
    Position pauseResumeVisit(PauseResumeHashTableVisitor& visitor,
                              Position& start_pos);

    /**
     * Return a position at the end of the hashtable. Has similar semantics
     * as STL end() (i.e. one past the last element).
     */
    Position endPosition() const;

    /**
     * Get the number of buckets that should be used for initialization.
     *
     * @param s if 0, return the default number of buckets, else return s
     */
    static size_t getNumBuckets(size_t s = 0);

    /**
     * Get the number of locks that should be used for initialization.
     *
     * @param s if 0, return the default number of locks, else return s
     */
    static size_t getNumLocks(size_t s);

    /**
     * Set the default number of buckets.
     */
    static void setDefaultNumBuckets(size_t);

    /**
     * Set the default number of locks.
     */
    static void setDefaultNumLocks(size_t);

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
    bool unlocked_ejectItem(StoredValue*& vptr, item_eviction_policy_t policy);

    std::atomic<uint64_t>     maxDeletedRevSeqno;
    std::atomic<size_t>       numTotalItems;
    std::atomic<size_t>       numNonResidentItems;
    std::atomic<size_t>       numEjects;
    //! Memory consumed by items in this hashtable.
    std::atomic<size_t>       memSize;
    //! Cache size.
    std::atomic<size_t>       cacheSize;
    //! Meta-data size.
    std::atomic<size_t>       metaDataMemory;

private:
    friend class StoredValue;

    inline bool isActive() const { return activeState; }
    inline void setActiveState(bool newv) { activeState = newv; }

    std::atomic<size_t> size;
    size_t               n_locks;
    StoredValue        **values;
    std::mutex               *mutexes;
    EPStats&             stats;
    StoredValueFactory   valFact;
    std::atomic<size_t>       visitors;
    std::atomic<size_t>       numItems;
    std::atomic<size_t>       numResizes;
    std::atomic<size_t>       numTempItems;
    bool                 activeState;

    static size_t                 defaultNumBuckets;
    static size_t                 defaultNumLocks;

    int getBucketForHash(int h) {
        return abs(h % static_cast<int>(size));
    }

    inline size_t mutexForBucket(size_t bucket_num) {
        if (!isActive()) {
            throw std::logic_error("HashTable::mutexForBucket: Cannot call on a "
                    "non-active object");
        }
        return bucket_num % n_locks;
    }

    Item *getRandomKeyFromSlot(int slot);

    DISALLOW_COPY_AND_ASSIGN(HashTable);
};

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
     */
    virtual void visit(StoredValue *v) = 0;

    /**
     * True if the visiting should continue.
     *
     * This is called periodically to ensure the visitor still wants
     * to visit items.
     */
    virtual bool shouldContinue() { return true; }
};

/**
 * Hash table visitor that collects stats of what's inside.
 */
class HashTableStatVisitor : public HashTableVisitor {
public:

    HashTableStatVisitor()
        : numNonResident(0), numTotal(0), memSize(0), valSize(0), cacheSize(0) {}

    void visit(StoredValue *v) {
        ++numTotal;
        memSize += v->size();
        valSize += v->valuelen();

        if (v->isResident()) {
            cacheSize += v->size();
        } else {
            ++numNonResident;
        }
    }

    size_t numNonResident;
    size_t numTotal;
    size_t memSize;
    size_t valSize;
    size_t cacheSize;
};

/**
 * Base class for visiting a hash table with pause/resume support.
 */
class PauseResumeHashTableVisitor {
public:
    /**
     * Visit an individual item within a hash table.
     *
     * @param v a pointer to a value in the hash table.
     * @return True if visiting should continue, otherwise false.
     */
    virtual bool visit(StoredValue& v) = 0;
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
