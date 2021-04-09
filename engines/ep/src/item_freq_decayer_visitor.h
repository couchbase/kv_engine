/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2018-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
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
    explicit ItemFreqDecayerVisitor(uint16_t percentage_);

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
