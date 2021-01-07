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

    bool run() noexcept override;

    void stop();

    std::string getDescription() override;

    std::chrono::microseconds maxExpectedDuration() override;

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
