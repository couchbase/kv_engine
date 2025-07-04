/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2015-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#pragma once

#include "ep_task.h"

#include <platform/semaphore.h>
#include <platform/semaphore_guard.h>
#include <string>

// Forward declaration.
class Configuration;
class EPStats;
class KVBucket;
class AccessScannerValueChangeListener;

class AccessScanner : public EpTask {
    friend class AccessScannerValueChangeListener;
public:
    AccessScanner(KVBucket& _store,
                  Configuration& conf,
                  EPStats& st,
                  double sleeptime = 0,
                  bool useStartTime = false,
                  bool completeBeforeShutdown = false);

    bool run() override;
    std::string getDescription() const override;
    std::chrono::microseconds maxExpectedDuration() const override;

    std::atomic<size_t> completedCount;

protected:
    void createAndScheduleTask(size_t shard,
                               cb::SemaphoreGuard<> semaphoreGuard);

    void updateAlogTime(double sleepSecs);

    /// Calculate the sleep time from now until the next alog_task_time
    double calculateSleepTime() const;

    KVBucket& store;
    Configuration& conf;
    EPStats& stats;
    double sleepTime;
    std::string alogPath;
    /**
     * semaphore is used to track creation and lifetime of the ItemAccessVisitor
     * tasks that are spawned by this (one per shard).
     */
    cb::Semaphore semaphore;
    uint64_t maxStoredItems;
};
