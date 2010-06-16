/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */

#include <cassert>
#include "stored-value.hh"

#ifndef DEFAULT_HT_SIZE
#define DEFAULT_HT_SIZE 12582917
#endif

#ifndef DEFAULT_MAX_DATA_SIZE
/* Something something something ought to be enough for anybody */
#define DEFAULT_MAX_DATA_SIZE (512L * 1024 * 1024 * 1024)
#endif

size_t HashTable::defaultNumBuckets = DEFAULT_HT_SIZE;
size_t HashTable::defaultNumLocks = 193;

size_t StoredValue::maxDataSize = DEFAULT_MAX_DATA_SIZE;
Atomic<size_t> StoredValue::currentSize;

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

void HashTable::clear() {
    assert(active);
    MultiLockHolder(mutexes, n_locks);
    for (int i = 0; i < (int)size; i++) {
        while (values[i]) {
            StoredValue *v = values[i];
            values[i] = v->next;
            delete v;
        }
        depths[i] = 0;
    }
}

void HashTable::visit(HashTableVisitor &visitor) {
    for (int i = 0; i < (int)size; i++) {
        LockHolder lh(getMutex(i));
        StoredValue *v = values[i];
        while (v) {
            visitor.visit(v);
            v = v->next;
        }
    }
}

void HashTable::visitDepth(HashTableDepthVisitor &visitor) {
    for (int i = 0; i < (int)size; i++) {
        LockHolder lh(getMutex(i));
        visitor.visit(i, depths[i]);
    }
}

/**
 * Get the maximum amount of memory available for storing data.
 *
 * @param n the current max data size
 *
 * @return the number of locks to create
 */
size_t StoredValue::getMaxDataSize() {
    return maxDataSize;
}

/**
 * Set the default number of bytes available for stored values.
 */
void StoredValue::setMaxDataSize(size_t to) {
    if (to != 0) {
        maxDataSize = to;
    }
}

/**
 * What's the total size of allocations?
 */
size_t StoredValue::getCurrentSize() {
    return currentSize.get();
}

void StoredValue::increaseCurrentSize(size_t by) {
    currentSize.incr(by);
}

void StoredValue::reduceCurrentSize(size_t by) {
    currentSize.decr(by);
    assert(static_cast<int64_t>(getCurrentSize()) >= 0);
}

/**
 * Is there enough space for this thing?
 */
bool StoredValue::hasAvailableSpace(const Item &item) {
    return getCurrentSize() + sizeof(StoredValue) + item.getNKey() + item.getNBytes()
        <= getMaxDataSize();
}
