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

#include "globaltask.h"

#include <string>

class KVBucketIface;

/**
 * Look around at hash tables and verify they're all sized
 * appropriately.
 */
class HashtableResizerTask : public GlobalTask {
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
        // very long.
        return std::chrono::milliseconds(10);
    }

private:
    KVBucketIface& store;
};
