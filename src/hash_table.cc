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

#include "hash_table.h"

#include <cstring>

#ifndef DEFAULT_HT_SIZE
#define DEFAULT_HT_SIZE 1531
#endif

size_t HashTable::defaultNumBuckets = DEFAULT_HT_SIZE;
size_t HashTable::defaultNumLocks = 193;

static ssize_t prime_size_table[] = {
    3, 7, 13, 23, 47, 97, 193, 383, 769, 1531, 3079, 6143, 12289, 24571, 49157,
    98299, 196613, 393209, 786433, 1572869, 3145721, 6291449, 12582917,
    25165813, 50331653, 100663291, 201326611, 402653189, 805306357,
    1610612741, -1
};


std::ostream& operator<<(std::ostream& os, const HashTable::Position& pos) {
    os << "{lock:" << pos.lock << " bucket:" << pos.hash_bucket << "/" << pos.ht_size << "}";
    return os;
}

HashTable::HashTable(EPStats &st, size_t s, size_t l)
    : maxDeletedRevSeqno(0),
      numTotalItems(0),
      numNonResidentItems(0),
      numEjects(0),
      memSize(0),
      cacheSize(0),
      metaDataMemory(0),
      stats(st),
      valFact(st),
      visitors(0),
      numItems(0),
      numResizes(0),
      numTempItems(0)
{
    size = HashTable::getNumBuckets(s);
    n_locks = HashTable::getNumLocks(l);
    values = static_cast<StoredValue**>(cb_calloc(size, sizeof(StoredValue*)));
    mutexes = new std::mutex[n_locks];
    activeState = true;
}

HashTable::~HashTable() {
    clear(true);
    // Wait for any outstanding visitors to finish.
    while (visitors > 0) {
#ifdef _MSC_VER
        Sleep(1);
#else
        usleep(100);
#endif
    }
    delete []mutexes;
    cb_free(values);
    values = NULL;
}

HashTableStatVisitor HashTable::clear(bool deactivate) {
    HashTableStatVisitor rv;

    if (!deactivate) {
        // If not deactivating, assert we're already active.
        if (!isActive()) {
            throw std::logic_error("HashTable::clear: Cannot call on a "
                    "non-active object");
        }
    }
    MultiLockHolder mlh(mutexes, n_locks);
    if (deactivate) {
        setActiveState(false);
    }
    for (int i = 0; i < (int)size; i++) {
        while (values[i]) {
            StoredValue *v = values[i];
            rv.visit(v);
            values[i] = v->next;
            delete v;
        }
    }

    stats.currentSize.fetch_sub(rv.memSize - rv.valSize);

    numTotalItems.store(0);
    numItems.store(0);
    numTempItems.store(0);
    numNonResidentItems.store(0);
    memSize.store(0);
    cacheSize.store(0);

    return rv;
}

static size_t distance(size_t a, size_t b) {
    return std::max(a, b) - std::min(a, b);
}

static size_t nearest(size_t n, size_t a, size_t b) {
    return (distance(n, a) < distance(b, n)) ? a : b;
}

static bool isCurrently(size_t size, ssize_t a, ssize_t b) {
    ssize_t current(static_cast<ssize_t>(size));
    return (current == a || current == b);
}

void HashTable::resize() {
    size_t ni = getNumInMemoryItems();
    int i(0);
    size_t new_size(0);

    // Figure out where in the prime table we are.
    ssize_t target(static_cast<ssize_t>(ni));
    for (i = 0; prime_size_table[i] > 0 && prime_size_table[i] < target; ++i) {
        // Just looking...
    }

    if (prime_size_table[i] == -1) {
        // We're at the end, take the biggest
        new_size = prime_size_table[i-1];
    } else if (prime_size_table[i] < static_cast<ssize_t>(defaultNumBuckets)) {
        // Was going to be smaller than the configured ht_size.
        new_size = defaultNumBuckets;
    } else if (0 == i) {
        new_size = prime_size_table[i];
    }else if (isCurrently(size, prime_size_table[i-1], prime_size_table[i])) {
        // If one of the candidate sizes is the current size, maintain
        // the current size in order to remain stable.
        new_size = size;
    } else {
        // Somewhere in the middle, use the one we're closer to.
        new_size = nearest(ni, prime_size_table[i-1], prime_size_table[i]);
    }

    resize(new_size);
}

void HashTable::resize(size_t newSize) {
    if (!isActive()) {
        throw std::logic_error("HashTable::resize: Cannot call on a "
                "non-active object");
    }

    // Due to the way hashing works, we can't fit anything larger than
    // an int.
    if (newSize > static_cast<size_t>(std::numeric_limits<int>::max())) {
        return;
    }

    // Don't resize to the same size, either.
    if (newSize == size) {
        return;
    }

    MultiLockHolder mlh(mutexes, n_locks);
    if (visitors.load() > 0) {
        // Do not allow a resize while any visitors are actually
        // processing.  The next attempt will have to pick it up.  New
        // visitors cannot start doing meaningful work (we own all
        // locks at this point).
        return;
    }

    // Get a place for the new items.
    StoredValue **newValues = static_cast<StoredValue**>(cb_calloc(newSize,
                                                        sizeof(StoredValue*)));
    // If we can't allocate memory, don't move stuff around.
    if (!newValues) {
        return;
    }

    stats.memOverhead.fetch_sub(memorySize());
    ++numResizes;

    // Set the new size so all the hashy stuff works.
    size_t oldSize = size;
    size.store(newSize);

    // Move existing records into the new space.
    for (size_t i = 0; i < oldSize; i++) {
        while (values[i]) {
            StoredValue *v = values[i];
            values[i] = v->next;

            int newBucket = getBucketForHash(hash(v->getKeyBytes(),
                                                  v->getKeyLen()));
            v->next = newValues[newBucket];
            newValues[newBucket] = v;
        }
    }

    // values still points to the old (now empty) table.
    cb_free(values);
    values = newValues;

    stats.memOverhead.fetch_add(memorySize());
}

StoredValue* HashTable::find(const std::string &key, bool trackReference) {
    if (!isActive()) {
        throw std::logic_error("HashTable::find: Cannot call on a "
                "non-active object");
    }
    int bucket_num(0);
    LockHolder lh = getLockedBucket(key, &bucket_num);
    return unlocked_find(key, bucket_num, false, trackReference);
}

Item* HashTable::getRandomKey(long rnd) {
    /* Try to locate a partition */
    size_t start = rnd % size;
    size_t curr = start;
    Item *ret;

    do {
        ret = getRandomKeyFromSlot(curr++);
        if (curr == size) {
            curr = 0;
        }
    } while (ret == NULL && curr != start);

    return ret;
}

mutation_type_t HashTable::set(Item &val, uint64_t cas,
                               bool allowExisting, bool hasMetaData,
                               item_eviction_policy_t policy) {
    int bucket_num(0);
    LockHolder lh = getLockedBucket(val.getKey(), &bucket_num);
    StoredValue *v = unlocked_find(val.getKey(), bucket_num, true, false);
    return unlocked_set(v, val, cas, allowExisting, hasMetaData, policy);
}

mutation_type_t HashTable::unlocked_set(StoredValue*& v, Item& itm,
                                        uint64_t cas, bool allowExisting,
                                        bool hasMetaData,
                                        item_eviction_policy_t policy,
                                        bool maybeKeyExists,
                                        bool isReplication) {
    if (!isActive()) {
        throw std::logic_error("HashTable::unlocked_set: Cannot call on a "
                "non-active object");
    }

    if (!StoredValue::hasAvailableSpace(stats, itm, isReplication)) {
        return NOMEM;
    }

    mutation_type_t rv = NOT_FOUND;

    if (cas && policy == FULL_EVICTION && maybeKeyExists) {
        if (!v || v->isTempInitialItem()) {
            return NEED_BG_FETCH;
        }
    }

    /*
     * prior to checking for the lock, we should check if this object
     * has expired. If so, then check if CAS value has been provided
     * for this set op. In this case the operation should be denied since
     * a cas operation for a key that doesn't exist is not a very cool
     * thing to do. See MB 3252
     */
    if (v && v->isExpired(ep_real_time()) && !hasMetaData) {
        if (v->isLocked(ep_current_time())) {
            v->unlock();
        }
        if (cas) {
            /* item has expired and cas value provided. Deny ! */
            return NOT_FOUND;
        }
    }

    if (v) {
        if (!allowExisting && !v->isTempItem()) {
            return INVALID_CAS;
        }
        if (v->isLocked(ep_current_time())) {
            /*
             * item is locked, deny if there is cas value mismatch
             * or no cas value is provided by the user
             */
            if (cas != v->getCas()) {
                return IS_LOCKED;
            }
            /* allow operation*/
            v->unlock();
        } else if (cas && cas != v->getCas()) {
            if (v->isTempDeletedItem() ||
                v->isTempNonExistentItem() ||
                v->isDeleted()) {
                return NOT_FOUND;
            }
            return INVALID_CAS;
        }

        rv = v->isClean() ? WAS_CLEAN : WAS_DIRTY;
        if (!v->isResident() && !v->isDeleted() && !v->isTempItem()) {
            decrNumNonResidentItems();
        }

        if (v->isTempItem()) {
            --numTempItems;
            ++numItems;
            ++numTotalItems;
        }

        v->setValue(itm, *this, hasMetaData /*Preserve revSeqno*/);

    } else if (cas != 0) {
        rv = NOT_FOUND;
    } else {
        int bucket_num = getBucketForHash(hash(itm.getKey()));
        v = valFact(itm, values[bucket_num], *this);
        values[bucket_num] = v;
        ++numItems;
        ++numTotalItems;

        if (!hasMetaData) {
            /**
             * Possibly, this item is being recreated. Conservatively assign it
             * a seqno that is greater than the greatest seqno of all deleted
             * items seen so far.
             */
            uint64_t seqno = getMaxDeletedRevSeqno() + 1;
            v->setRevSeqno(seqno);
            itm.setRevSeqno(seqno);
        }
        rv = WAS_CLEAN;
    }
    return rv;
}

mutation_type_t HashTable::insert(Item &itm, item_eviction_policy_t policy,
                                  bool eject, bool partial) {
    if (!isActive()) {
        throw std::logic_error("HashTable::insert: Cannot call on a "
                "non-active object");
    }
    if (!StoredValue::hasAvailableSpace(stats, itm)) {
        return NOMEM;
    }

    int bucket_num(0);
    LockHolder lh = getLockedBucket(itm.getKey(), &bucket_num);
    StoredValue *v = unlocked_find(itm.getKey(), bucket_num, true, false);

    if (v == NULL) {
        v = valFact(itm, values[bucket_num], *this);
        v->markClean();
        if (partial) {
            v->markNotResident();
            ++numNonResidentItems;
        }
        values[bucket_num] = v;
        ++numItems;
        v->setNewCacheItem(false);
    } else {
        if (partial) {
            // We don't have a better error code ;)
            return INVALID_CAS;
        }

        // Verify that the CAS isn't changed
        if (v->getCas() != itm.getCas()) {
            if (v->getCas() == 0) {
                v->cas = itm.getCas();
                v->flags = itm.getFlags();
                v->exptime = itm.getExptime();
                v->revSeqno = itm.getRevSeqno();
            } else {
                return INVALID_CAS;
            }
        }

        if (!v->isResident() && !v->isDeleted() && !v->isTempItem()) {
            decrNumNonResidentItems();
        }

        if (v->isTempItem()) {
            --numTempItems;
            ++numItems;
            ++numTotalItems;
        }

        v->setValue(const_cast<Item&>(itm), *this, true);
    }

    v->markClean();

    if (eject && !partial) {
        unlocked_ejectItem(v, policy);
    }

    return NOT_FOUND;
}

add_type_t HashTable::add(Item &val, item_eviction_policy_t policy,
                          bool isDirty) {
    if (!isActive()) {
        throw std::logic_error("HashTable::add: Cannot call on a "
                "non-active object");
    }
    int bucket_num(0);
    LockHolder lh = getLockedBucket(val.getKey(), &bucket_num);
    StoredValue *v = unlocked_find(val.getKey(), bucket_num, true, false);
    return unlocked_add(bucket_num, v, val, policy, isDirty,
                        /*maybeKeyExists*/true, /*isReplication*/false);
}

add_type_t HashTable::unlocked_add(int &bucket_num,
                                   StoredValue*& v,
                                   Item &itm,
                                   item_eviction_policy_t policy,
                                   bool isDirty,
                                   bool maybeKeyExists,
                                   bool isReplication) {
    add_type_t rv = ADD_SUCCESS;
    if (v && !v->isDeleted() && !v->isExpired(ep_real_time()) &&
       !v->isTempItem()) {
        rv = ADD_EXISTS;
    } else {
        if (!StoredValue::hasAvailableSpace(stats, itm,
                                            isReplication)) {
            return ADD_NOMEM;
        }

        if (v) {
            if (v->isTempInitialItem() && policy == FULL_EVICTION
                && maybeKeyExists) {
                // Need to figure out if an item exists on disk
                return ADD_BG_FETCH;
            }

            rv = (v->isDeleted() || v->isExpired(ep_real_time())) ?
                                   ADD_UNDEL : ADD_SUCCESS;
            if (v->isTempItem()) {
                if (v->isTempDeletedItem()) {
                    itm.setRevSeqno(v->getRevSeqno() + 1);
                } else {
                    itm.setRevSeqno(getMaxDeletedRevSeqno() + 1);
                }
                --numTempItems;
                ++numItems;
                ++numTotalItems;
            }
            v->setValue(itm, *this, v->isTempItem() ? true : false);
            if (isDirty) {
                v->markDirty();
            } else {
                v->markClean();
            }
        } else {
            if (itm.getBySeqno() != StoredValue::state_temp_init) {
                if (policy == FULL_EVICTION && maybeKeyExists) {
                    return ADD_TMP_AND_BG_FETCH;
                }
            }
            v = valFact(itm, values[bucket_num], *this, isDirty);
            values[bucket_num] = v;

            if (v->isTempItem()) {
                ++numTempItems;
                rv = ADD_BG_FETCH;
            } else {
                ++numItems;
                ++numTotalItems;
            }

            /**
             * Possibly, this item is being recreated. Conservatively assign
             * it a seqno that is greater than the greatest seqno of all
             * deleted items seen so far.
             */
            uint64_t seqno = 0;
            if (!v->isTempItem()) {
                seqno = getMaxDeletedRevSeqno() + 1;
            } else {
                seqno = getMaxDeletedRevSeqno();
            }
            v->setRevSeqno(seqno);
            itm.setRevSeqno(seqno);
        }

        if (v && v->isTempItem()) {
            v->setNRUValue(MAX_NRU_VALUE);
        }
    }

    return rv;
}

add_type_t HashTable::unlocked_addTempItem(int &bucket_num,
                                           const std::string &key,
                                           item_eviction_policy_t policy,
                                           bool isReplication) {

    if (!isActive()) {
        throw std::logic_error("HashTable::unlocked_addTempItem: Cannot call on a "
                "non-active object");
    }
    uint8_t ext_meta[1];
    uint8_t ext_len = EXT_META_LEN;
    *(ext_meta) = PROTOCOL_BINARY_RAW_BYTES;
    Item itm(key.c_str(), key.length(), /*flags*/0, /*exp*/0, /*data*/NULL,
             /*size*/0, ext_meta, ext_len, 0, StoredValue::state_temp_init);

    // if a temp item for a possibly deleted, set it non-resident by resetting
    // the value cuz normally a new item added is considered resident which
    // does not apply for temp item.
    StoredValue* v = NULL;
    return unlocked_add(bucket_num, v, itm, policy,
                        false,  // isDirty
                        true,   // maybeKeyExists
                        isReplication);
}

mutation_type_t HashTable::softDelete(const std::string &key, uint64_t cas,
                                      item_eviction_policy_t policy) {
    if (!isActive()) {
        throw std::logic_error("HashTable::softDelete: Cannot call on a "
                "non-active object");
    }
    int bucket_num(0);
    LockHolder lh = getLockedBucket(key, &bucket_num);
    StoredValue *v = unlocked_find(key, bucket_num, false, false);
    return unlocked_softDelete(v, cas, policy);
}

mutation_type_t HashTable::unlocked_softDelete(StoredValue *v,
                                               uint64_t cas,
                                               item_eviction_policy_t policy) {
    ItemMetaData metadata;
    if (v) {
        metadata.revSeqno = v->getRevSeqno() + 1;
    }
    return unlocked_softDelete(v, cas, metadata, policy);
}

mutation_type_t HashTable::unlocked_softDelete(StoredValue *v,
                                               uint64_t cas,
                                               ItemMetaData &metadata,
                                               item_eviction_policy_t policy,
                                               bool use_meta) {
    mutation_type_t rv = NOT_FOUND;

    if ((!v || v->isTempInitialItem()) && policy == FULL_EVICTION) {
        return NEED_BG_FETCH;
    }

    if (v) {
        if (v->isExpired(ep_real_time()) && !use_meta) {
            if (!v->isResident() && !v->isDeleted() && !v->isTempItem()) {
                decrNumNonResidentItems();
            }
            v->setRevSeqno(metadata.revSeqno);
            v->del(*this);
            updateMaxDeletedRevSeqno(v->getRevSeqno());
            return rv;
        }

        if (v->isLocked(ep_current_time())) {
            if (cas != v->getCas()) {
                return IS_LOCKED;
            }
            v->unlock();
        }

        if (cas != 0 && cas != v->getCas()) {
            return INVALID_CAS;
        }

        if (!v->isResident() && !v->isDeleted() && !v->isTempItem()) {
            decrNumNonResidentItems();
        }

        if (v->isTempItem()) {
            --numTempItems;
            ++numItems;
            ++numTotalItems;
        }

        /* allow operation*/
        v->unlock();

        rv = v->isClean() ? WAS_CLEAN : WAS_DIRTY;
        v->setRevSeqno(metadata.revSeqno);
        if (use_meta) {
            v->setCas(metadata.cas);
            v->setFlags(metadata.flags);
            v->setExptime(metadata.exptime);
        }
        v->del(*this);
        updateMaxDeletedRevSeqno(v->getRevSeqno());
    }
    return rv;
}

StoredValue* HashTable::unlocked_find(const std::string &key, int bucket_num,
                                      bool wantsDeleted, bool trackReference) {
    StoredValue *v = values[bucket_num];
    while (v) {
        if (v->hasKey(key)) {
            if (trackReference && !v->isDeleted()) {
                v->referenced();
            }
            if (wantsDeleted || !v->isDeleted()) {
                return v;
            } else {
                return NULL;
            }
        }
        v = v->next;
    }
    return NULL;
}

bool HashTable::unlocked_del(const std::string &key, int bucket_num) {
    if (!isActive()) {
        throw std::logic_error("HashTable::unlocked_del: Cannot call on a "
                "non-active object");
    }
    StoredValue *v = values[bucket_num];

    // Special case empty bucket.
    if (!v) {
        return false;
    }

    // Special case the first one
    if (v->hasKey(key)) {
        if (!v->isDeleted() && v->isLocked(ep_current_time())) {
            return false;
        }

        values[bucket_num] = v->next;
        StoredValue::reduceCacheSize(*this, v->size());
        StoredValue::reduceMetaDataSize(*this, stats, v->metaDataSize());
        if (v->isTempItem()) {
            --numTempItems;
        } else {
            decrNumItems();
            decrNumTotalItems();
        }
        delete v;
        return true;
    }

    while (v->next) {
        if (v->next->hasKey(key)) {
            StoredValue *tmp = v->next;
            if (!tmp->isDeleted() && tmp->isLocked(ep_current_time())) {
                return false;
            }

            v->next = v->next->next;
            StoredValue::reduceCacheSize(*this, tmp->size());
            StoredValue::reduceMetaDataSize(*this, stats, tmp->metaDataSize());
            if (tmp->isTempItem()) {
                --numTempItems;
            } else {
                decrNumItems();
                decrNumTotalItems();
            }
            delete tmp;
            return true;
        } else {
            v = v->next;
        }
    }

    return false;
}

void HashTable::visit(HashTableVisitor &visitor) {
    if ((numItems.load() + numTempItems.load()) == 0 || !isActive()) {
        return;
    }

    // Acquire one (any) of the mutexes before incrementing {visitors}, this
    // prevents any race between this visitor and the HashTable resizer.
    // See comments in pauseResumeVisit() for further details.
    LockHolder lh(mutexes[0]);
    VisitorTracker vt(&visitors);
    lh.unlock();

    bool aborted = !visitor.shouldContinue();
    size_t visited = 0;
    for (int l = 0; isActive() && !aborted && l < static_cast<int>(n_locks);
         l++) {
        for (int i = l; i < static_cast<int>(size); i+= n_locks) {
            // (re)acquire mutex on each HashBucket, to minimise any impact
            // on front-end threads.
            LockHolder lh(mutexes[l]);

            StoredValue *v = values[i];
            if (v) {
                // TODO: Perf: This check seems costly - do we think it's still
                // worth keeping?
                auto hashbucket = getBucketForHash(hash(v->getKeyBytes(),
                                                        v->getKeyLen()));
                if (i != hashbucket) {
                    throw std::logic_error("HashTable::visit: inconsistency "
                            "between StoredValue's calculated hashbucket "
                            "(which is " + std::to_string(hashbucket) +
                            ") and bucket is is located in (which is " +
                            std::to_string(i) + ")");
                }
            }
            while (v) {
                StoredValue *tmp = v->next;
                visitor.visit(v);
                v = tmp;
            }
            ++visited;
        }
        lh.unlock();
        aborted = !visitor.shouldContinue();
    }
}

void HashTable::visitDepth(HashTableDepthVisitor &visitor) {
    if (numItems.load() == 0 || !isActive()) {
        return;
    }
    size_t visited = 0;
    VisitorTracker vt(&visitors);

    for (int l = 0; l < static_cast<int>(n_locks); l++) {
        LockHolder lh(mutexes[l]);
        for (int i = l; i < static_cast<int>(size); i+= n_locks) {
            size_t depth = 0;
            StoredValue *p = values[i];
            if (p) {
                // TODO: Perf: This check seems costly - do we think it's still
                // worth keeping?
                auto hashbucket = getBucketForHash(hash(p->getKeyBytes(),
                                                        p->getKeyLen()));
                if (i != hashbucket) {
                    throw std::logic_error("HashTable::visit: inconsistency "
                            "between StoredValue's calculated hashbucket "
                            "(which is " + std::to_string(hashbucket) +
                            ") and bucket it is located in (which is " +
                            std::to_string(i) + ")");
                }
            }
            size_t mem(0);
            while (p) {
                depth++;
                mem += p->size();
                p = p->next;
            }
            visitor.visit(i, depth, mem);
            ++visited;
        }
    }
}

HashTable::Position
HashTable::pauseResumeVisit(PauseResumeHashTableVisitor& visitor,
                            Position& start_pos) {
    if ((numItems.load() + numTempItems.load()) == 0 || !isActive()) {
        // Nothing to visit
        return endPosition();
    }

    bool paused = false;

    // To attempt to minimize the impact the visitor has on normal frontend
    // operations, we deliberately acquire (and release) the mutex between
    // each hash_bucket - see `lh` in the inner for() loop below. This means we
    // hold a given mutex for a large number of short durations, instead of just
    // one single, long duration.
    // *However*, there is a potential race with this approach - the {size} of
    // the HashTable may be changed (by the Resizer task) between us first
    // reading it to calculate the starting hash_bucket, and then reading it
    // inside the inner for() loop. To prevent this race, we explicitly acquire
    // (any) mutex, increment {visitors} and then release the mutex. This
    //avoids the race as if visitors >0 then Resizer will not attempt to resize.
    LockHolder lh(mutexes[0]);
    VisitorTracker vt(&visitors);
    lh.unlock();

    // Start from the requested lock number if in range.
    size_t lock = (start_pos.lock < n_locks) ? start_pos.lock : 0;
    size_t hash_bucket = 0;

    for (; isActive() && !paused && lock < n_locks; lock++) {

        // If the bucket position is *this* lock, then start from the
        // recorded bucket (as long as we haven't resized).
        hash_bucket = lock;
        if (start_pos.lock == lock &&
            start_pos.ht_size == size &&
            start_pos.hash_bucket < size) {
            hash_bucket = start_pos.hash_bucket;
        }

        // Iterate across all values in the hash buckets owned by this lock.
        // Note: we don't record how far into the bucket linked-list we
        // pause at; so any restart will begin from the next bucket.
        for (; !paused && hash_bucket < size; hash_bucket += n_locks) {
            LockHolder lh(mutexes[lock]);

            StoredValue *v = values[hash_bucket];
            while (!paused && v) {
                StoredValue *tmp = v->next;
                paused = !visitor.visit(*v);
                v = tmp;
            }
        }

        // If the visitor paused us before we visited all hash buckets owned
        // by this lock, we don't want to skip the remaining hash buckets, so
        // stop the outer for loop from advancing to the next lock.
        if (paused && hash_bucket < size) {
            break;
        }

        // Finished all buckets owned by this lock. Set hash_bucket to 'size'
        // to give a consistent marker for "end of lock".
        hash_bucket = size;
    }

    // Return the *next* location that should be visited.
    return HashTable::Position(size, lock, hash_bucket);
}

HashTable::Position HashTable::endPosition() const  {
    return HashTable::Position(size, n_locks, size);
}

static inline size_t getDefault(size_t x, size_t d) {
    return x == 0 ? d : x;
}

size_t HashTable::getNumBuckets(size_t n) {
    return getDefault(n, defaultNumBuckets);
}

size_t HashTable::getNumLocks(size_t n) {
    return getDefault(n, defaultNumLocks);
}

void HashTable::setDefaultNumBuckets(size_t to) {
    if (to != 0) {
        defaultNumBuckets = to;
    }
}

void HashTable::setDefaultNumLocks(size_t to) {
    if (to != 0) {
        defaultNumLocks = to;
    }
}

bool HashTable::unlocked_ejectItem(StoredValue*& vptr,
                                   item_eviction_policy_t policy) {
    if (vptr == nullptr) {
        throw std::invalid_argument("HashTable::unlocked_ejectItem: "
                "Unable to delete NULL StoredValue");
    }

    if (policy == VALUE_ONLY) {
        bool rv = vptr->ejectValue(*this, policy);
        if (rv) {
            ++stats.numValueEjects;
            ++numNonResidentItems;
            ++numEjects;
            return true;
        } else {
            ++stats.numFailedEjects;
            return false;
        }
    } else { // full eviction.
        if (vptr->eligibleForEviction(policy)) {
            StoredValue::reduceMetaDataSize(*this, stats,
                                            vptr->metaDataSize());
            StoredValue::reduceCacheSize(*this, vptr->size());

            int bucket_num = getBucketForHash(hash(vptr->getKey()));
            StoredValue *v = values[bucket_num];
            // Remove the item from the hash table.
            if (v == vptr) {
                values[bucket_num] = v->next;
            } else {
                while (v->next) {
                    if (v->next == vptr) {
                        v->next = v->next->next;
                        break;
                    } else {
                        v = v->next;
                    }
                }
            }

            if (vptr->isResident()) {
                ++stats.numValueEjects;
            }
            if (!vptr->isResident() && !v->isTempItem()) {
                decrNumNonResidentItems(); // Decrement because the item is
                                           // fully evicted.
            }
            decrNumItems(); // Decrement because the item is fully evicted.
            ++numEjects;
            updateMaxDeletedRevSeqno(vptr->getRevSeqno());

            delete vptr; // Free the item.
            vptr = NULL;
            return true;
        } else {
            ++stats.numFailedEjects;
            return false;
        }
    }
}

Item *HashTable::getRandomKeyFromSlot(int slot) {
    LockHolder lh = getLockedBucket(slot);
    StoredValue *v = values[slot];

    while (v) {
        if (!v->isTempItem() && !v->isDeleted() && v->isResident()) {
            return v->toItem(false, 0);
        }
        v = v->next;
    }

    return NULL;
}
