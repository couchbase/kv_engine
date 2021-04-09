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

#include "globaltask.h"
#include "kv_bucket_iface.h"

class ItemCompressorVisitor;
class EPStats;
class PauseResumeVBAdapter;

/**
 * Task responsible for compressing items in memory.
 */
class ItemCompressorTask : public GlobalTask {
public:
    ItemCompressorTask(EventuallyPersistentEngine* e, EPStats& stats_);

    bool run() override;

    void stop();

    std::string getDescription() const override;

    std::chrono::microseconds maxExpectedDuration() const override;

private:
    /// Duration (in seconds) the compressor should sleep for between
    /// iterations.
    double getSleepTime() const;

    // Upper limit on how long each item compressor chunk can run for, before
    // being paused.
    std::chrono::milliseconds getChunkDuration() const;

    /// Returns the underlying ItemCompressorVisitor instance.
    ItemCompressorVisitor& getItemCompressorVisitor();

    /// Reference to EP stats, used to check on mem_used.
    EPStats& stats;

    // Opaque marker indicating how far through the epStore we have visited.
    KVBucketIface::Position epstore_position;

    /**
     * Visitor adapter which supports pausing & resuming (records how far
     * though a VBucket is has got). unique_ptr as we re-create it for each
     * complete pass.
     */
    std::unique_ptr<PauseResumeVBAdapter> prAdapter;
};
