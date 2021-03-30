/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2021 Couchbase, Inc
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

#include "vbucket_fwd.h"

#include <memcached/dockey.h>

#include <string>

#include <functional>

namespace CollectionEntry {
struct Entry;
}

#pragma once

/**
 * RAII helper to check the per-collection stats changes in the expected
 * manner.
 *
 * Currently used through MemChecker (checking mem_used) and DiskChecker
 * (checking disk_size).
 *
 * Checks that the stat when the helper is destroyed vs when it was
 * created meets the given invariant. E.g.,
 *  {
 *      auto x = MemChecker(*vb, CollectionEntry::defaultC, std::greater<>());
 *      // do something which should change the mem used of the default
 *      // collection as tracked by the hash table statistics.
 *  }
 *
 * This checks that when `x` goes out of scope, the memory usage of the default
 * collection is _greater_ than when the checker was constructed.
 */
class StatChecker {
public:
    using PostFunc = std::function<bool(size_t, size_t)>;

    /**
     * Create a StatChecker object that on destruction compares stats to the
     * on creation state. Takes a VBucketPtr reference, from which we gather
     * stats, which also allows us to use the StatChecker across different
     * VBucket objects. The use case for this is warmup tests which can create
     * a StatChecker against the pre-warmup VBucket object and then reset the
     * VBucketPtr to the post-warmup VBucket object to test stats after a
     * restart.
     */
    StatChecker(VBucketPtr& vb,
                const CollectionEntry::Entry& entry,
                std::function<size_t(VBucketPtr&, CollectionID)> getter,
                std::string statName,
                PostFunc postCondition);

    virtual ~StatChecker();

protected:
    size_t getValue();

    VBucketPtr& vb;
    const CollectionEntry::Entry& entry;
    std::function<size_t(VBucketPtr&, CollectionID)> getter;
    std::string statName;
    PostFunc postCondition;
    size_t initialValue = 0;
};

class MemChecker : public StatChecker {
public:
    MemChecker(VBucketPtr& vb,
               const CollectionEntry::Entry& entry,
               PostFunc postCondition);
};

class DiskChecker : public StatChecker {
public:
    DiskChecker(VBucketPtr& vb,
                const CollectionEntry::Entry& entry,
                PostFunc postCondition);
};

size_t getCollectionMemUsed(VBucketPtr& vb, CollectionID cid);
size_t getCollectionDiskSize(VBucketPtr& vb, CollectionID cid);
