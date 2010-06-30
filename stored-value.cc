/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */

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

void HashTable::clear() {
    assert(active());
    MultiLockHolder(mutexes, n_locks);
    for (int i = 0; i < (int)size; i++) {
        while (values[i]) {
            StoredValue *v = values[i];
            values[i] = v->next;
            delete v;
        }
    }
}

void HashTable::visit(HashTableVisitor &visitor) {

    size_t visited = 0;
    bool aborted = !visitor.shouldContinue();
    for (int l = 0; !aborted && l < static_cast<int>(n_locks); l++) {
        LockHolder lh(getMutex(l));
        for (int i = l; i < static_cast<int>(size); i+= n_locks) {
            assert(l == mutexForBucket(i));
            StoredValue *v = values[i];
            while (v) {
                visitor.visit(v);
                v = v->next;
            }
            visited++;
        }
        lh.unlock();
        aborted = !visitor.shouldContinue();
    }
    assert(aborted || visited == size);
}

void HashTable::visitDepth(HashTableDepthVisitor &visitor) {

    size_t visited = 0;
    for (int l = 0; l < static_cast<int>(n_locks); l++) {
        LockHolder lh(getMutex(l));
        for (int i = l; i < static_cast<int>(size); i+= n_locks) {
            assert(l == mutexForBucket(i));
            size_t depth(0);
            StoredValue *v = values[i];
            while (v) {
                ++depth;
                v = v->next;
            }
            visitor.visit(i, depth);
            visited++;
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
