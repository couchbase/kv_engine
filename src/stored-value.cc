/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#include "config.h"
#include <cassert>
#include <limits>

#include "stored-value.hh"

#ifndef DEFAULT_HT_SIZE
#define DEFAULT_HT_SIZE 1531
#endif

size_t HashTable::defaultNumBuckets = DEFAULT_HT_SIZE;
size_t HashTable::defaultNumLocks = 193;
enum stored_value_type HashTable::defaultStoredValueType = featured;
double StoredValue::mutation_mem_threshold = 0.9;
const int64_t StoredValue::state_id_cleared = -1;
const int64_t StoredValue::state_id_pending = -2;
const int64_t StoredValue::state_deleted_key = -3;
const int64_t StoredValue::state_non_existent_key = -4;
const int64_t StoredValue::state_temp_init = -5;

static ssize_t prime_size_table[] = {
    3, 7, 13, 23, 47, 97, 193, 383, 769, 1531, 3067, 6143, 12289, 24571, 49157,
    98299, 196613, 393209, 786433, 1572869, 3145721, 6291449, 12582917,
    25165813, 50331653, 100663291, 201326611, 402653189, 805306357,
    1610612741, -1
};

bool StoredValue::ejectValue(EPStats &stats, HashTable &ht) {
    if (eligibleForEviction()) {
        size_t oldsize = size();
        size_t old_valsize = value->length();
        blobval uval;
        uval.len = valLength();
        value_t sp(Blob::New(uval.chlen, sizeof(uval)));
        extra.resident = false;
        timestampEviction();
        value = sp;
        size_t newsize = size();
        size_t new_valsize = value->length();

        // ejecting the value may increase the object size....
        if (oldsize < newsize) {
            increaseCacheSize(ht, newsize - oldsize);
        } else if (newsize < oldsize) {
            reduceCacheSize(ht, oldsize - newsize);
        }
        // Add or substract the key/meta data overhead differenece.
        size_t old_keymeta_overhead = (oldsize - old_valsize);
        size_t new_keymeta_overhead = (newsize - new_valsize);
        if (old_keymeta_overhead < new_keymeta_overhead) {
            increaseCurrentSize(stats, new_keymeta_overhead - old_keymeta_overhead);
        } else if (new_keymeta_overhead < old_keymeta_overhead) {
            reduceCurrentSize(stats, old_keymeta_overhead - new_keymeta_overhead);
        }
        ++stats.numValueEjects;
        ++ht.numNonResidentItems;
        ++ht.numEjects;
        if (isReferenced()) {
            ++ht.numReferencedEjects;
        }
        return true;
    }
    ++stats.numFailedEjects;
    return false;
}

void StoredValue::referenced(HashTable &ht) {
    if (extra.nru == false) {
        extra.nru = true;
        ++ht.numReferenced;
    }
}

bool StoredValue::isReferenced(bool reset, HashTable *ht) {
    bool ret = extra.nru;
    if (reset && extra.nru) {
        extra.nru = false;
        assert(ht);
        --ht->numReferenced;
    }
    return ret;
}

bool StoredValue::unlocked_restoreValue(Item *itm, EPStats &stats,
                                        HashTable &ht) {
    // If cas == we loaded the object from our meta file, but
    // we didn't know the size of the object.. Don't report
    // this as an unexpected size change.
    if (getCas() == 0) {
        extra.cas = itm->getCas();
        flags = itm->getFlags();
        extra.exptime = itm->getExptime();
        extra.seqno = itm->getSeqno();
        setValue(*itm, stats, ht, true);
        if (!isResident()) {
            --ht.numNonResidentItems;
        }
        markClean(NULL);
        return true;
    }

    if (!isResident()) {
        size_t oldsize = size();
        size_t old_valsize = isDeleted() ? 0 : value->length();
        if (itm->getValue()->length() != valLength()) {
            if (valLength()) {
                // generate warning msg only if valLength() > 0, otherwise,
                // no warning is necessary since it is the very first
                // background fetch after warmup.
                int diff(static_cast<int>(valLength()) - // expected
                        static_cast<int>(itm->getValue()->length())); // got
                getLogger()->log(EXTENSION_LOG_WARNING, NULL,
                        "Object unexpectedly changed size by %d bytes\n",
                        diff);
            }
        }

        rel_time_t evicted_time(getEvictedTime());
        stats.pagedOutTimeHisto.add(ep_current_time() - evicted_time);
        extra.resident = true;
        value = itm->getValue();

        size_t newsize = size();
        size_t new_valsize = value->length();
        if (oldsize < newsize) {
            increaseCacheSize(ht, newsize - oldsize);
        } else if (newsize < oldsize) {
            reduceCacheSize(ht, oldsize - newsize);
        }
        // Add or substract the key/meta data overhead differenece.
        size_t old_keymeta_overhead = (oldsize - old_valsize);
        size_t new_keymeta_overhead = (newsize - new_valsize);
        if (old_keymeta_overhead < new_keymeta_overhead) {
            increaseCurrentSize(stats, new_keymeta_overhead - old_keymeta_overhead);
        } else if (new_keymeta_overhead < old_keymeta_overhead) {
            reduceCurrentSize(stats, old_keymeta_overhead - new_keymeta_overhead);
        }
        --ht.numNonResidentItems;
        return true;
    }
    return false;
}

mutation_type_t HashTable::insert(const Item &itm, bool eject, bool partial) {
    assert(isActive());
    if (!StoredValue::hasAvailableSpace(stats, itm)) {
        return NOMEM;
    }

    assert(itm.getCas() != static_cast<uint64_t>(-1));

    int bucket_num(0);
    LockHolder lh = getLockedBucket(itm.getKey(), &bucket_num);
    StoredValue *v = unlocked_find(itm.getKey(), bucket_num, true, false);

    if (v == NULL) {
        v = valFact(itm, values[bucket_num], *this);
        v->markClean(NULL);
        if (partial) {
            v->extra.resident = false;
            ++numNonResidentItems;
        }
        values[bucket_num] = v;
        ++numItems;
    } else {
        if (partial) {
            // We don't have a better error code ;)
            return INVALID_CAS;
        }

        // Verify that the CAS isn't changed
        if (v->getCas() != itm.getCas()) {
            if (v->getCas() == 0) {
                v->extra.cas = itm.getCas();
                v->flags = itm.getFlags();
                v->extra.exptime = itm.getExptime();
                v->extra.seqno = itm.getSeqno();
            } else {
                return INVALID_CAS;
            }
        }
        if (!v->isResident() && !v->isDeleted()) {
            --numNonResidentItems;
        }
        v->setValue(const_cast<Item&>(itm), stats, *this, true);
    }

    v->markClean(NULL);

    if (eject && !partial) {
        v->ejectValue(stats, *this);
    }

    return NOT_FOUND;

}

bool StoredValue::unlocked_restoreMeta(Item *itm, ENGINE_ERROR_CODE status) {
    assert(state_deleted_key != getId() && state_non_existent_key != getId());
    switch(status) {
    case ENGINE_SUCCESS:
        assert(0 == itm->getValue()->length());
        setSeqno(itm->getSeqno());
        setCas(itm->getCas());
        flags = itm->getFlags();
        setExptime(itm->getExptime());
        setStoredValueState(state_deleted_key);
        return true;
    case ENGINE_KEY_ENOENT:
        setStoredValueState(state_non_existent_key);
        return true;
    default:
        getLogger()->log(
            EXTENSION_LOG_WARNING, NULL,
            "The underlying storage returned error %d for get_meta\n", status);
        return false;
    }
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

/**
 * Set the default number of hashtable buckets.
 */
void HashTable::setDefaultNumBuckets(size_t to) {
    if (to != 0) {
        defaultNumBuckets = to;
    }
}

/**
 * Set the default number of hashtable locks.
 */
void HashTable::setDefaultNumLocks(size_t to) {
    if (to != 0) {
        defaultNumLocks = to;
    }
}

HashTableStatVisitor HashTable::clear(bool deactivate) {
    HashTableStatVisitor rv;

    if (!deactivate) {
        // If not deactivating, assert we're already active.
        assert(isActive());
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

    stats.currentSize.decr(rv.memSize - rv.valSize);
    assert(stats.currentSize.get() < GIGANTOR);

    numItems.set(0);
    numTempItems.set(0);
    numNonResidentItems.set(0);
    memSize.set(0);
    cacheSize.set(0);
    numReferenced.set(0);
    numReferencedEjects.set(0);

    return rv;
}

void HashTable::resize(size_t newSize) {
    assert(isActive());

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
    if (visitors.get() > 0) {
        // Do not allow a resize while any visitors are actually
        // processing.  The next attempt will have to pick it up.  New
        // visitors cannot start doing meaningful work (we own all
        // locks at this point).
        return;
    }

    // Get a place for the new items.
    StoredValue **newValues = static_cast<StoredValue**>(calloc(newSize,
                                                                sizeof(StoredValue*)));
    // If we can't allocate memory, don't move stuff around.
    if (!newValues) {
        return;
    }

    stats.memOverhead.decr(memorySize());
    ++numResizes;

    // Set the new size so all the hashy stuff works.
    size_t oldSize = size;
    size = newSize;
    ep_sync_synchronize();

    // Move existing records into the new space.
    for (size_t i = 0; i < oldSize; i++) {
        while (values[i]) {
            StoredValue *v = values[i];
            values[i] = v->next;

            int newBucket = getBucketForHash(hash(v->getKeyBytes(), v->getKeyLen()));
            v->next = newValues[newBucket];
            newValues[newBucket] = v;
        }
    }

    // values still points to the old (now empty) table.
    free(values);
    values = newValues;

    stats.memOverhead.incr(memorySize());
    assert(stats.memOverhead.get() < GIGANTOR);
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
    size_t ni = getNumItems();
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
    } else if (isCurrently(size, prime_size_table[i-1], prime_size_table[i])) {
        // If one of the candidate sizes is the current size, maintain
        // the current size in order to remain stable.
        new_size = size;
    } else {
        // Somewhere in the middle, use the one we're closer to.
        new_size = nearest(ni, prime_size_table[i-1], prime_size_table[i]);
    }

    resize(new_size);
}

void HashTable::visit(HashTableVisitor &visitor) {
    if ((numItems.get() + numTempItems.get()) == 0 || !isActive()) {
        return;
    }
    VisitorTracker vt(&visitors);
    bool aborted = !visitor.shouldContinue();
    size_t visited = 0;
    for (int l = 0; isActive() && !aborted && l < static_cast<int>(n_locks); l++) {
        LockHolder lh(mutexes[l]);
        for (int i = l; i < static_cast<int>(size); i+= n_locks) {
            assert(l == mutexForBucket(i));
            StoredValue *v = values[i];
            assert(v == NULL || i == getBucketForHash(hash(v->getKeyBytes(),
                                                           v->getKeyLen())));
            while (v) {
                visitor.visit(v);
                v = v->next;
            }
            ++visited;
        }
        lh.unlock();
        aborted = !visitor.shouldContinue();
    }
    assert(aborted || visited == size);
}

void HashTable::visitDepth(HashTableDepthVisitor &visitor) {
    if (numItems.get() == 0 || !isActive()) {
        return;
    }
    size_t visited = 0;
    VisitorTracker vt(&visitors);

    for (int l = 0; l < static_cast<int>(n_locks); l++) {
        LockHolder lh(mutexes[l]);
        for (int i = l; i < static_cast<int>(size); i+= n_locks) {
            size_t depth = 0;
            StoredValue *p = values[i];
            assert(p == NULL || i == getBucketForHash(hash(p->getKeyBytes(),
                                                           p->getKeyLen())));
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

    assert(visited == size);
}

add_type_t HashTable::unlocked_add(int &bucket_num,
                                   const Item &val,
                                   bool isDirty,
                                   bool storeVal,
                                   bool trackReference) {
    StoredValue *v = unlocked_find(val.getKey(), bucket_num,
                                   true, trackReference);
    add_type_t rv = ADD_SUCCESS;
    if (v && !v->isDeleted() && !v->isExpired(ep_real_time())) {
        rv = ADD_EXISTS;
    } else {
        Item &itm = const_cast<Item&>(val);
        itm.setCas();
        if (!StoredValue::hasAvailableSpace(stats, itm)) {
            return ADD_NOMEM;
        }
        if (v) {
            rv = (v->isDeleted() || v->isExpired(ep_real_time())) ? ADD_UNDEL : ADD_SUCCESS;
            v->setValue(itm, stats, *this, false);
            if (isDirty) {
                v->markDirty();
            } else {
                v->markClean(NULL);
            }
        } else {
            v = valFact(itm, values[bucket_num], *this, isDirty);
            values[bucket_num] = v;

            if (v->isTempItem()) {
                ++numTempItems;
            } else {
                ++numItems;
            }

            /**
             * Possibly, this item is being recreated. Conservatively assign
             * it a seqno that is greater than the greatest seqno of all
             * deleted items seen so far.
             */
            uint64_t seqno = getMaxDeletedSeqno() + 1;
            v->setSeqno(seqno);
            itm.setSeqno(seqno);
        }
        if (!storeVal) {
            v->ejectValue(stats, *this);
        }
        if (v->isTempItem()) {
            v->resetValue();
        } else {
            v->referenced(*this);
        }
    }

    return rv;
}

add_type_t HashTable::unlocked_addTempDeletedItem(int &bucket_num,
                                                  const std::string &key) {

    assert(isActive());
    Item itm(key.c_str(), key.length(), (size_t)0, (uint32_t)0, (time_t)0,
             0, StoredValue::state_temp_init);

    // if a temp item for a possibly deleted, set it non-resident by resetting
    // the value cuz normally a new item added is considered resident which does
    // not apply for temp item.

    return unlocked_add(bucket_num, itm,
                        false,  // isDirty
                        true,   // storeVal
                        false); // trackReference
}

void StoredValue::setMutationMemoryThreshold(double memThreshold) {
    if (memThreshold > 0.0 && memThreshold <= 1.0) {
        mutation_mem_threshold = memThreshold;
    }
}

void StoredValue::increaseCacheSize(HashTable &ht, size_t by) {
    ht.cacheSize.incr(by);
    assert(ht.cacheSize.get() < GIGANTOR);
    ht.memSize.incr(by);
    assert(ht.memSize.get() < GIGANTOR);
}

void StoredValue::reduceCacheSize(HashTable &ht, size_t by) {
    ht.cacheSize.decr(by);
    assert(ht.cacheSize.get() < GIGANTOR);
    ht.memSize.decr(by);
    assert(ht.memSize.get() < GIGANTOR);
}

void StoredValue::increaseCurrentSize(EPStats &st, size_t by) {
    st.currentSize.incr(by);
    assert(st.currentSize.get() < GIGANTOR);
}

void StoredValue::reduceCurrentSize(EPStats &st, size_t by) {
    size_t val;
    do {
        val = st.currentSize.get();
        assert(val >= by);
    } while (!st.currentSize.cas(val, val - by));;
}

void StoredValue::increaseMetaDataSize(HashTable &ht, size_t by) {
    ht.metaDataMemory.incr(by);
    assert(ht.metaDataMemory.get() < GIGANTOR);
}

void StoredValue::reduceMetaDataSize(HashTable &ht, size_t by) {
    ht.metaDataMemory.decr(by);
    assert(ht.metaDataMemory.get() < GIGANTOR);
}

/**
 * Is there enough space for this thing?
 */
bool StoredValue::hasAvailableSpace(EPStats &st, const Item &itm) {
    double newSize = static_cast<double>(st.getTotalMemoryUsed() +
                                         sizeof(StoredValue) + itm.getNKey());
    double maxSize=  static_cast<double>(st.getMaxDataSize()) * mutation_mem_threshold;
    return newSize <= maxSize;
}

Item* StoredValue::toItem(bool lck, uint16_t vbucket) const {
    return new Item(getKey(), getFlags(), getExptime(),
                    value,
                    lck ? static_cast<uint64_t>(-1) : getCas(),
                    id, vbucket, getSeqno());
}
