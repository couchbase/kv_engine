/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2014 Couchbase, Inc
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

#include "hash_table.h"
#include "item.h"
#include "progress_tracker.h"
#include "vb_visitors.h"
#include "vbucket.h"

/**
 * Defragmentation visitor - visit all objects in a VBucket, compress the
 * documents and defragment any which have reached the specified age.
 */
class DefragmentVisitor : public VBucketAwareHTVisitor {
public:
    DefragmentVisitor(uint8_t age_threshold_,
                      size_t max_size_class,
                      boost::optional<uint8_t> sv_age_threshold);

    ~DefragmentVisitor();

    // Set the deadline at which point the visitor will pause visiting.
    void setDeadline(ProcessClock::time_point deadline_);

    // Implementation of HashTableVisitor interface:
    virtual bool visit(const HashTable::HashBucketLock& lh,
                       StoredValue& v) override;

    // Resets any held stats to zero.
    void clearStats();

    // Returns the number of documents that have been defragmented.
    size_t getDefragCount() const;

    // Returns the number of documents that have been visited.
    size_t getVisitedCount() const;

    // Returns the number of StoredValues that have been defragmented.
    size_t getStoredValueDefragCount() const;

    void setCurrentVBucket(VBucket& vb) override;

private:
    /// Request to reallocate the StoredValue
    void defragmentStoredValue(StoredValue& v) const;

    /* Configuration parameters */

    // Size of the largest size class from the allocator.
    const size_t max_size_class;

    // How old a blob must be to consider it for defragmentation.
    const uint8_t age_threshold;

    /* Runtime state */

    // Estimates how far we have got, and when we should pause.
    ProgressTracker progressTracker;

    /* Statistics */
    // Count of how many documents have been defrag'd.
    size_t defrag_count;
    // How many documents have been visited.
    size_t visited_count;
    // How many stored-values have been defrag'd
    mutable size_t sv_defrag_count{0};

    // The current vbucket that is being processed
    VBucket* currentVb;

    // If defined, the age at which StoredValue's are de-fragmented
    boost::optional<uint8_t> sv_age_threshold;
};
