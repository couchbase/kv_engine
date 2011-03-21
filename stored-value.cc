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

static ssize_t prime_size_table[] = {
    3, 7, 13, 23, 47, 97, 193, 383, 769, 1531, 3067, 6143, 12289, 24571, 49157,
    98299, 196613, 393209, 786433, 1572869, 3145721, 6291449, 12582917,
    25165813, 50331653, 100663291, 201326611, 402653189, 805306357,
    1610612741, -1
};

bool StoredValue::ejectValue(EPStats &stats, HashTable &ht) {
    if (isResident() && isClean() && !isDeleted() && !_isSmall) {
        size_t oldsize = size();
        size_t oldValueSize = value->length();
        blobval uval;
        uval.len = valLength();
        shared_ptr<Blob> sp(Blob::New(uval.chlen, sizeof(uval)));
        extra.feature.resident = false;
        value = sp;
        size_t newsize = size();
        size_t newValueSize = value->length();

        // ejecting the value may increase the object size....
        if (oldsize < newsize) {
            increaseCacheSize(ht, newsize - oldsize, true);
        } else if (newsize < oldsize) {
            reduceCacheSize(ht, oldsize - newsize, true);
        }
        if ((oldsize - oldValueSize) < (newsize - newValueSize)) {
            increaseCurrentSize(stats, (newsize - newValueSize) - (oldsize - oldValueSize));
        } else if ((newsize - newValueSize) < (oldsize - oldValueSize)) {
            reduceCurrentSize(stats, (oldsize - oldValueSize) - (newsize - newValueSize));
        }
        ++stats.numValueEjects;
        ++ht.numNonResidentItems;
        ++ht.numEjects;
        return true;
    }
    ++stats.numFailedEjects;
    return false;
}

bool StoredValue::restoreValue(value_t v, EPStats &stats, HashTable &ht) {
    if (!isResident()) {
        size_t oldsize = size();
        size_t oldValueSize = isDeleted() ? 0 : value->length();
        assert(v);
        if (v->length() != valLength()) {
            int diff(static_cast<int>(valLength()) - // expected
                     static_cast<int>(v->length())); // got
            getLogger()->log(EXTENSION_LOG_WARNING, NULL,
                             "Object unexpectedly changed size by %d bytes\n",
                             diff);
        }
        extra.feature.resident = true;
        value = v;

        size_t newsize = size();
        size_t newValueSize = value->length();
        if (oldsize < newsize) {
            increaseCacheSize(ht, newsize - oldsize, true);
        } else if (newsize < oldsize) {
            reduceCacheSize(ht, oldsize - newsize, true);
        }
        if ((oldsize - oldValueSize) < (newsize - newValueSize)) {
            increaseCurrentSize(stats, (newsize - newValueSize) - (oldsize - oldValueSize));
        } else if ((newsize - newValueSize) < (oldsize - oldValueSize)) {
            reduceCurrentSize(stats, (oldsize - oldValueSize) - (newsize - newValueSize));
        }
        --ht.numNonResidentItems;
        return true;
    }
    return false;
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
        assert(active());
    }
    MultiLockHolder mlh(mutexes, n_locks);
    if (deactivate) {
        active(false);
    }
    for (int i = 0; i < (int)size; i++) {
        while (values[i]) {
            StoredValue *v = values[i];
            rv.visit(v);
            values[i] = v->next;
            delete v;
        }
    }

    numItems.set(0);
    numNonResidentItems.set(0);
    memSize.set(0);
    cacheSize.set(0);

    return rv;
}

void HashTable::resize(size_t newSize) {
    assert(active());

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
    if (numItems.get() == 0 || !active()) {
        return;
    }
    VisitorTracker vt(&visitors);
    bool aborted = !visitor.shouldContinue();
    size_t visited = 0;
    for (int l = 0; active() && !aborted && l < static_cast<int>(n_locks); l++) {
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
    if (numItems.get() == 0 || !active()) {
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

bool HashTable::setDefaultStorageValueType(const char *t) {
    bool rv = false;
    if (t && strcmp(t, "featured") == 0) {
        setDefaultStorageValueType(featured);
        rv = true;
    } else if (t && strcmp(t, "small") == 0) {
        setDefaultStorageValueType(small);
        rv = true;
    }
    return rv;
}

void HashTable::setDefaultStorageValueType(enum stored_value_type t) {
    defaultStoredValueType = t;
}

enum stored_value_type HashTable::getDefaultStorageValueType() {
    return defaultStoredValueType;
}

const char* HashTable::getDefaultStorageValueTypeStr() {
    const char *rv = "unknown";
    switch(getDefaultStorageValueType()) {
    case small: rv = "small"; break;
    case featured: rv = "featured"; break;
    default: abort();
    }
    return rv;
}

/**
 * Get the maximum amount of memory available for storing data.
 *
 * @return the memory ceiling
 */
size_t StoredValue::getMaxDataSize(EPStats& st) {
    return st.maxDataSize;
}

/**
 * Set the default number of bytes available for stored values.
 */
void StoredValue::setMaxDataSize(EPStats &st, size_t to) {
    if (to != 0) {
        st.maxDataSize = to;
    }
}

/**
 * What's the total size of allocations?
 */
size_t StoredValue::getCurrentSize(EPStats &st) {
    return st.currentSize.get() + st.memOverhead.get();
}

void StoredValue::increaseCacheSize(HashTable &ht,
                                    size_t by, bool residentOnly) {
    if (!residentOnly) {
        ht.cacheSize.incr(by);
        assert(ht.cacheSize.get() < GIGANTOR);
    }
    ht.memSize.incr(by);
    assert(ht.memSize.get() < GIGANTOR);
}

void StoredValue::reduceCacheSize(HashTable &ht,
                                  size_t by, bool residentOnly) {
    if (!residentOnly) {
        ht.cacheSize.decr(by);
        assert(ht.cacheSize.get() < GIGANTOR);
    }
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

/**
 * Is there enough space for this thing?
 */
bool StoredValue::hasAvailableSpace(EPStats &st, const Item &item) {
    return getCurrentSize(st) + sizeof(StoredValue) + item.getNKey() // + item.getNBytes()
        <= getMaxDataSize(st);
}
