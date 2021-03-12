/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2019 Couchbase, Inc
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
#include "vb_visitors.h"
#include <platform/atomic_duration.h>

/*
 * Enforces the Durability Timeout for the SyncWrites tracked in this KVBucket.
 */
class DurabilityTimeoutTask : public GlobalTask {
public:
    class ConfigChangeListener;

    /**
     * @param engine The engine that will be visited
     */
    DurabilityTimeoutTask(EventuallyPersistentEngine& engine,
                          std::chrono::milliseconds interval);

    bool run() override;

    std::string getDescription() const override {
        return "DurabilityTimeoutTask";
    }

    std::chrono::microseconds maxExpectedDuration() const override {
        // This tasks just spawns a new VBCBAdaptor, which is the actual tasks
        // that executes the DurabilityTimeoutVisitor. So, keeping the value
        // relatively high as there is no too much value in logging this timing.
        return std::chrono::seconds(1);
    }

    void setSleepTime(std::chrono::milliseconds value) {
        sleepTime = value;
    }

private:
    // Note: this is the actual minimum interval between subsequent runs.
    // The VBCBAdaptor (which is the actual task that executes this Visitor)
    // has its internal sleep-time which is used for a different purpose,
    // details in VBCBAdaptor.
    cb::AtomicDuration<std::chrono::milliseconds,
                       std::memory_order::memory_order_seq_cst>
            sleepTime;
};

/**
 * DurabilityTimeoutVisitor visits a VBucket for enforcing the Durability
 * Timeout for the SyncWrites tracked by VBucket.
 */
class DurabilityTimeoutVisitor : public CappedDurationVBucketVisitor {
public:
    DurabilityTimeoutVisitor() : startTime(std::chrono::steady_clock::now()) {
    }

    void visitBucket(const VBucketPtr& vb) override;

private:
    const std::chrono::steady_clock::time_point startTime;
};
