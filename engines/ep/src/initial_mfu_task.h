/*
 *     Copyright 2022-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#pragma once

#include <executor/globaltask.h>
#include <chrono>

/**
 * The InitialMFUTask runs every X seconds, iterates all vBuckets and aggregates
 * their MFU histograms to determine what the initial MFU value of items
 * inserted into the HashTable should be.
 *
 * The task only reschedules for the upfront_mfu_only eviction strategy. If the
 * strategy is not upfront_mfu_only, or if the strategy changes at runtime,
 * the task should be woken up manually.
 */
class InitialMFUTask : public GlobalTask {
public:
    InitialMFUTask(EventuallyPersistentEngine& e);

    std::string getDescription() const override;

    bool run() override;

    std::chrono::microseconds maxExpectedDuration() const override;
};
