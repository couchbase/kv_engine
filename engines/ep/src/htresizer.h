/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2010 Couchbase, Inc
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

    std::string getDescription() override {
        return "Adjusting hash table sizes.";
    }

    std::chrono::microseconds maxExpectedDuration() override {
        // [per Bucket Task] This task doesn't do very much (most of the actual
        // work to check and resize HashTables is delegated to the per-vBucket
        // 'ResizingVisitor' tasks). As such we don't expect to execute for
        // very long.
        return std::chrono::milliseconds(10);
    }

private:
    KVBucketIface& store;
};
