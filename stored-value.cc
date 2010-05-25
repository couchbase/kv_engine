/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */

#include <cassert>
#include "stored-value.hh"

size_t HashTable::defaultNumBuckets = 196613;
size_t HashTable::defaultNumLocks = 193;

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
