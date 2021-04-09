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

#include "globaltask.h"
#include "kvstore.h"
#include "storeddockey.h"

#include <array>
#include <chrono>
#include <sstream>
#include <string>

class EPBucket;
class EventuallyPersistentEngine;

/**
 * A task for persisting items to disk.
 */
class Flusher;
class FlusherTask : public GlobalTask {
public:
    FlusherTask(EventuallyPersistentEngine *e, Flusher* f, uint16_t shardid,
                bool completeBeforeShutdown = true)
        : GlobalTask(e, TaskId::FlusherTask, 0, completeBeforeShutdown),
          flusher(f) {
        std::stringstream ss;
        ss<<"Running a flusher loop: shard "<<shardid;
        desc = ss.str();
    }

    bool run() override;

    std::string getDescription() const override {
        return desc;
    }

    std::chrono::microseconds maxExpectedDuration() const override {
        // Flusher duration is likely to vary significantly; depending on
        // number of documents to flush and speed/capacity of disk subsystem.
        // As such, selecting a good maximum duration for all scenarios is hard.
        // Choose a relatively generous value of 1s - this should record
        // any significantly slow executions without creating too much log
        // noise.
        return std::chrono::seconds(1);
    }

private:
    Flusher* flusher;
    std::string desc;
};

/**
 * A task for compacting a vbucket db file
 */
class CompactTask : public GlobalTask {
public:
    CompactTask(EPBucket& bucket,
                Vbid vbid,
                std::optional<CompactionConfig> config,
                const void* ck,
                bool completeBeforeShutdown = false);

    bool run() override;

    std::string getDescription() const override;

    std::chrono::microseconds maxExpectedDuration() const override {
        // Empirical evidence suggests this task runs under 25s 99.98% of
        // the time.
        return std::chrono::seconds(25);
    }

    /**
     * This function should be called only when the task is already exists and
     * is "scheduled".
     *
     * The caller does not need to know the state of the task, it could be:
     * A: waiting in the scheduler
     * B: running (i.e. already executing a compaction)
     *
     * The config parameter is optional, if a config is specified then
     * compaction will run with the given config. If a config is not specified
     * then compaction will run with the current config. When a config is
     * specified the input is merged with the current config
     * (see CompactionConfig::merge)
     *
     * If the ordering means that A is true, then when compaction does run the
     * latest config will be used.
     *
     * If the ordering means that B is true, then the task will reschedule once
     * the current compaction is complete, the latest config will be used in the
     * reschedule run.
     *
     * @return The config that the task is now configured with (e.g. merged
     *         config) is returned.
     */
    CompactionConfig runCompactionWithConfig(
            std::optional<CompactionConfig> config, const void* cookie);

    /**
     * @return true if a reschedule is required
     */
    bool isRescheduleRequired() const;

    /**
     * @return the CompactionConfig for the task
     */
    CompactionConfig getCurrentConfig() const;

    /**
     * Set a callback that is invoked when the task can be considered to be
     * compacting, that is after preDoCompact has been called.
     */
    void setRunningCallback(std::function<void()> callback) {
        runningCallback = callback;
    }

private:
    /**
     * @return a copy of the current config and clear rescheduleRequired
     */
    std::pair<CompactionConfig, std::vector<const void*>> preDoCompact();

    /**
     * Using the input cookies and current "Compaction" state, determine if
     * the task is done. The task may have cookies to notify (compaction could
     * not run this time) or the task was notified to run again.
     *
     * @param cookies Any cookies which haven't been notified should be passed
     *        via this parameter for re-insertion into the Compaction state.
     *        This results in a reschedule for a future run.
     *
     * @return true if the task reschedule for a future run.
     */
    bool isTaskDone(const std::vector<const void*>& cookies);

    EPBucket& bucket;
    Vbid vbid;

    struct Compaction {
        CompactionConfig config{};
        std::vector<const void*> cookiesWaiting;
        bool rescheduleRequired{false};
    };

    folly::Synchronized<Compaction> compaction;
    std::function<void()> runningCallback;
};

/**
 * A task that periodically takes a snapshot of the stats and persists them to
 * disk.
 */
class StatSnap : public GlobalTask {
public:
    explicit StatSnap(EventuallyPersistentEngine* e)
        : GlobalTask(e,
                     TaskId::StatSnap,
                     /*sleeptime*/ 0,
                     /*completeBeforeShutdown*/ false) {
    }

    bool run() override;

    std::string getDescription() const override {
        return "Updating stat snapshot on disk";
    }

    std::chrono::microseconds maxExpectedDuration() const override {
        // A background periodic Writer task; which no front-end operation
        // depends on. However it does run on a writer thread; which we don't
        // want to slow down persistTo times; so expect to complete quickly.
        // p99.9 at 250ms.
        // TODO: Consider moving this to AuxIO?
        return std::chrono::milliseconds(250);
    }
};

/**
 * A task for fetching items from disk.
 */
class BgFetcher;
class MultiBGFetcherTask : public GlobalTask {
public:
    MultiBGFetcherTask(EventuallyPersistentEngine* e, BgFetcher* b);

    bool run() override;

    std::string getDescription() const override {
        return "Batching background fetch";
    }

    std::chrono::microseconds maxExpectedDuration() const override {
        // Much like other disk tasks (e.g. Flusher), duration is likely to
        // vary significantly; depending on number of documents to fetch and
        // speed/capacity of disk subsystem. As such, selecting a good maximum
        // duration for all scenarios is hard.
        // Choose a relatively generous value of 700ms - this should record
        // any significantly slow executions without creating too much log
        // noise.
        return std::chrono::milliseconds(700);
    }

private:
    BgFetcher *bgfetcher;
};

/**
 * A task for performing disk fetches for "stats vkey".
 */
class VKeyStatBGFetchTask : public GlobalTask {
public:
    VKeyStatBGFetchTask(EventuallyPersistentEngine* e,
                        const DocKey& k,
                        Vbid vbid,
                        uint64_t s,
                        const void* c,
                        int sleeptime = 0,
                        bool completeBeforeShutdown = false)
        : GlobalTask(e,
                     TaskId::VKeyStatBGFetchTask,
                     sleeptime,
                     completeBeforeShutdown),
          key(k),
          vbucket(vbid),
          bySeqNum(s),
          cookie(c),
          description("Fetching item from disk for vkey stat: key{" +
                      std::string(key.c_str()) + "} " + vbucket.to_string()) {
    }

    bool run() override;

    std::string getDescription() const override {
        return description;
    }

    std::chrono::microseconds maxExpectedDuration() const override {
        // Much like other disk tasks, duration is likely to
        // vary significantly; depending on speed/capacity of disk subsystem.
        // As such, selecting a good maximum duration for all scenarios is hard.
        // Choose a relatively generous value of 250ms - this should record
        // any significantly slow executions without creating too much log
        // noise.
        return std::chrono::milliseconds(250);
    }

private:
    const StoredDocKey key;
    const Vbid vbucket;
    uint64_t                         bySeqNum;
    const void                      *cookie;
    const std::string description;
};

/**
 * A task that monitors if a bucket is read-heavy, write-heavy, or mixed.
 */
class WorkLoadMonitor : public GlobalTask {
public:
    explicit WorkLoadMonitor(EventuallyPersistentEngine* e,
                             bool completeBeforeShutdown = false);

    bool run() override;

    std::chrono::microseconds maxExpectedDuration() const override {
        // Runtime should be very quick (lookup a few statistics; perform
        // some calculation on them). p99.9 is <50us.
        return std::chrono::milliseconds(1);
    }

    std::string getDescription() const override {
        return "Monitoring a workload pattern";
    }

private:

    size_t getNumMutations();
    size_t getNumGets();

    size_t prevNumMutations;
    size_t prevNumGets;
};
