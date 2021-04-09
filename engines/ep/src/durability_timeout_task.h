/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2019-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
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
