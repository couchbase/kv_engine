/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2015 Couchbase, Inc.
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

    std::string getDescription() override {
        return desc;
    }

    std::chrono::microseconds maxExpectedDuration() override {
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
                const CompactionConfig& c,
                const void* ck,
                bool completeBeforeShutdown = false);

    bool run() override;

    std::string getDescription() override;

    std::chrono::microseconds maxExpectedDuration() override {
        // Empirical evidence suggests this task runs under 25s 99.98% of
        // the time.
        return std::chrono::seconds(25);
    }

private:
    EPBucket& bucket;
    Vbid vbid;
    CompactionConfig compactionConfig;
    const void* cookie;
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

    std::string getDescription() override {
        return "Updating stat snapshot on disk";
    }

    std::chrono::microseconds maxExpectedDuration() override {
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

    std::string getDescription() override {
        return "Batching background fetch";
    }

    std::chrono::microseconds maxExpectedDuration() override {
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

    std::string getDescription() override {
        return description;
    }

    std::chrono::microseconds maxExpectedDuration() override {
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

    std::chrono::microseconds maxExpectedDuration() override {
        // Runtime should be very quick (lookup a few statistics; perform
        // some calculation on them). p99.9 is <50us.
        return std::chrono::milliseconds(1);
    }

    std::string getDescription() override {
        return "Monitoring a workload pattern";
    }

private:

    size_t getNumMutations();
    size_t getNumGets();

    size_t prevNumMutations;
    size_t prevNumGets;
};
