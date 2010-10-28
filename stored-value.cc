/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#include "config.h"
#include <cassert>
#include "stored-value.hh"

#ifndef DEFAULT_HT_SIZE
#define DEFAULT_HT_SIZE 12582917
#endif

size_t HashTable::defaultNumBuckets = DEFAULT_HT_SIZE;
size_t HashTable::defaultNumLocks = 193;
enum stored_value_type HashTable::defaultStoredValueType = featured;

static inline size_t getDefault(size_t x, size_t d) {
    return x == 0 ? d : x;
}

/**
 * Get the number of buckets for a hash table.
 *
 * @param n the desired number of buckets, if 0, use the default
 *
 * @return the number of buckets to create
 */
size_t HashTable::getNumBuckets(size_t n = 0) {
    return getDefault(n, defaultNumBuckets);
}

/**
 * Get the number of locks for a hash table.
 *
 * @param n the desired number of locks, if 0, use the default
 *
 * @return the number of locks to create
 */
size_t HashTable::getNumLocks(size_t n = 0) {
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

    return rv;
}

void HashTable::visit(HashTableVisitor &visitor) {
    if (numItems.get() == 0 || !active()) {
        return;
    }
    VisitorTracker vt(&visitors);
    bool aborted = !visitor.shouldContinue();
    size_t visited = 0;
    for (int l = 0; active() && !aborted && l < static_cast<int>(n_locks); l++) {
        LockHolder lh(getMutex(l));
        for (int i = l; i < static_cast<int>(size); i+= n_locks) {
            assert(l == mutexForBucket(i));
            StoredValue *v = values[i];
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
        LockHolder lh(getMutex(l));
        for (int i = l; i < static_cast<int>(size); i+= n_locks) {
            size_t depth = 0;
            StoredValue *p = values[i];
            while (p) {
                depth++;
                p = p->next;
            }
            visitor.visit(i, depth);
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

size_t StoredValue::getTotalCacheSize(EPStats &st) {
    return st.totalCacheSize.get();
}

void StoredValue::increaseCurrentSize(EPStats &st, size_t by, bool residentOnly) {
    if (!residentOnly) {
        st.totalCacheSize.incr(by);
    }
    st.currentSize.incr(by);
    assert(st.currentSize.get() < GIGANTOR);
}

void StoredValue::reduceCurrentSize(EPStats &st, size_t by, bool residentOnly) {
    size_t val;

    do {
        val = st.currentSize.get();
        assert(val >= by);
    } while (!st.currentSize.cas(val, val - by));;

    if (!residentOnly) {
        st.totalCacheSize.decr(by);
    }
}

/**
 * Is there enough space for this thing?
 */
bool StoredValue::hasAvailableSpace(EPStats &st, const Item &item) {
    return getCurrentSize(st) + sizeof(StoredValue) + item.getNKey() + item.getNBytes()
        <= getMaxDataSize(st);
}
