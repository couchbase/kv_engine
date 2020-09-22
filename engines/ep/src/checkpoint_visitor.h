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
    size_t removed;
    std::chrono::steady_clock::time_point taskStart;

    /**
     * Flag used to identify if memory usage was above the backfill threshold
     * when the CheckpointVisitor started. Used to determine if we have to wake
     * up snoozed backfills at CheckpointVisitor completion.
     */
    bool wasAboveBackfillThreshold;

    std::atomic<bool>& stateFinalizer;
};
