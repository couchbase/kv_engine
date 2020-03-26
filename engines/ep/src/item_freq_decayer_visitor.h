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

/**
 * Visit all documents in a hash table and reduce the frequency count of each
 * document by a given percentage.
 */
class ItemFreqDecayerVisitor : public VBucketAwareHTVisitor {
public:
    ItemFreqDecayerVisitor(uint16_t percentage_);

    ~ItemFreqDecayerVisitor() override = default;

    // Set the deadline at which point the visitor will pause visiting.
    void setDeadline(std::chrono::steady_clock::time_point deadline_);

    // The visit function will decay the frequency counter of each document
    // in the hash table by the percentage set when the visitor was
    // constructed.
    bool visit(const HashTable::HashBucketLock& lh, StoredValue& v) override;

    // Resets any held stats to zero.
    void clearStats();

    // Returns the number of documents that have been visited.
    size_t getVisitedCount() const;

private:
    /* Configuration parameters */

    // The percentage by which to age (i.e. reduce) the frequency counter
    // A value of 0 would means no change to the counter, whilst a value of
    // 100 would reset the counter to zero.
    const uint16_t percentage;

    /* Runtime state */

    // Estimates how far we have got, and when we should pause.
    ProgressTracker progressTracker;

    /* Statistics */
    // How many documents have been visited.
    size_t visitedCount;
};
