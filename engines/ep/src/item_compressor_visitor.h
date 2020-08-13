/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2018 Couchbase, Inc
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

#include "hash_table.h"
#include "progress_tracker.h"
#include "vb_visitors.h"
#include <memcached/engine.h>

/**
 * Item Compressor visitor - visit all objects in a VBucket and compress
 * the values
 */
class ItemCompressorVisitor : public VBucketAwareHTVisitor {
public:
    ItemCompressorVisitor();

    ~ItemCompressorVisitor() override;

    // Set the deadline at which point the visitor will pause visiting.
    void setDeadline(std::chrono::steady_clock::time_point deadline_);

    // Set the current bucket compression mode
    void setCompressionMode(const BucketCompressionMode compressionMode);

    // Set the minimum compression ratio
    void setMinCompressionRatio(float minCompressionRatio);

    // Implementation of HashTableVisitor interface:
    bool visit(const HashTable::HashBucketLock& lh, StoredValue& v) override;

    // Resets any held stats to zero.
    void clearStats();

    // Returns the number of documents that have been compressed.
    size_t getCompressedCount() const;

    // Returns the number of documents that have been visited.
    size_t getVisitedCount() const;

    void setCurrentVBucket(VBucket& vb) override;

private:
    /* Runtime state */

    // Estimates how far we have got, and when we should pause.
    ProgressTracker progressTracker;

    /* Statistics */
    // Count of how many documents have been compressed.
    size_t compressed_count;
    // How many documents have been visited.
    size_t visited_count;

    // Current compression mode of the bucket
    BucketCompressionMode compressMode;

    // The current vbucket that is being processed
    VBucket* currentVb;

    // The current minimum compression ratio supported by the bucket
    float currentMinCompressionRatio;
};
