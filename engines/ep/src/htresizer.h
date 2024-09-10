/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2010-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#pragma once

#include "ep_task.h"
#include <string>

class KVBucketIface;

/**
 * Look around at hash tables and verify they're all sized
 * appropriately.
 */
class HashtableResizerTask : public EpTask {
public:

    HashtableResizerTask(KVBucketIface& s, double sleepTime);

    bool run() override;

    std::string getDescription() const override {
        return "Adjusting hash table sizes.";
    }

    std::chrono::microseconds maxExpectedDuration() const override {
        // [per Bucket Task] This task doesn't do very much (most of the actual
        // work to check and resize HashTables is delegated to the per-vBucket
        // 'ResizingVisitor' tasks). As such we don't expect to execute for
        // very long. 15ms set from real runtime histogram from Morpheus builds
        // running on perf clusters where 99.9 is at 12ms
        return std::chrono::milliseconds(15);
    }

private:
    KVBucketIface& store;
};
