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

#include "vb_visitors.h"

#include <atomic>
#include <chrono>

class KVBucketIface;

/**
 * Remove all the closed unreferenced checkpoints for each vbucket.
 */
class CheckpointVisitor : public CappedDurationVBucketVisitor {
public:
    /**
     * Construct a CheckpointVisitor.
     */
    CheckpointVisitor(KVBucketIface* s, EPStats& st, std::atomic<bool>& sfin);

    void visitBucket(const VBucketPtr& vb) override;

    void complete() override;

private:
    KVBucketIface* store;
    EPStats& stats;
    std::chrono::steady_clock::time_point taskStart;

    /**
     * Flag used to identify if memory usage was above the backfill threshold
     * when the CheckpointVisitor started. Used to determine if we have to wake
     * up snoozed backfills at CheckpointVisitor completion.
     */
    bool wasAboveBackfillThreshold;

    std::atomic<bool>& stateFinalizer;
};
