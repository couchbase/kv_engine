/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2013-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#include "tasks.h"
#include "bgfetcher.h"
#include "bucket_logger.h"
#include "ep_bucket.h"
#include "ep_engine.h"
#include "executorpool.h"
#include "flusher.h"
#include "warmup.h"

#include <climits>
#include <type_traits>

#include <phosphor/phosphor.h>

static const double WORKLOAD_MONITOR_FREQ(5.0);

bool FlusherTask::run() {
    return flusher->step(this);
}

CompactTask::CompactTask(EPBucket& bucket,
                         Vbid vbid,
                         std::optional<CompactionConfig> config,
                         const CookieIface* ck,
                         bool completeBeforeShutdown)
    : GlobalTask(&bucket.getEPEngine(),
                 TaskId::CompactVBucketTask,
                 0,
                 completeBeforeShutdown),
      bucket(bucket),
      vbid(vbid) {
    auto lockedState = compaction.wlock();

    if (config) {
        lockedState->config = config.value();
    }

    if (ck) {
        lockedState->cookiesWaiting.push_back(ck);
    }
}

bool CompactTask::run() {
    TRACE_EVENT1("ep-engine/task", "CompactTask", "file_id", vbid.get());

    // pull out the config we have been requested to run with and any cookies
    // that maybe waiting.
    auto compactionData = preDoCompact();

    if (runningCallback) {
        runningCallback();
    }

    /**
     * MB-30015: Check to see if tombstones that have invalid
     * data needs to be retained. The goal is to try and retain
     * the erroneous tombstones especially in customer environments
     * for further analysis
     */
    compactionData.first.retain_erroneous_tombstones =
            bucket.isRetainErroneousTombstones();

    auto reschedule =
            bucket.doCompact(vbid, compactionData.first, compactionData.second);

    bucket.updateCompactionTasks(vbid, !reschedule /*canErase if !reschedule*/);

    return isTaskDone(compactionData.second) || reschedule;
}

std::string CompactTask::getDescription() const {
    return "Compact DB file " + std::to_string(vbid.get());
}

bool CompactTask::isRescheduleRequired() const {
    return compaction.rlock()->rescheduleRequired;
}

bool CompactTask::isTaskDone(const std::vector<const CookieIface*>& cookies) {
    auto handle = compaction.wlock();
    bool value = handle->rescheduleRequired;
    handle->rescheduleRequired = false;
    // Any cookies remaining must be retained for a future run
    handle->cookiesWaiting.insert(
            handle->cookiesWaiting.end(), cookies.begin(), cookies.end());
    return value || !handle->cookiesWaiting.empty();
}

CompactionConfig CompactTask::getCurrentConfig() const {
    return compaction.rlock()->config;
}

std::pair<CompactionConfig, std::vector<const CookieIface*>>
CompactTask::preDoCompact() {
    auto lockedState = compaction.wlock();
    lockedState->rescheduleRequired = false;
    // config and cookiesWaiting are moved out to the task run loop, any
    // subsequent schedule now operates on the 'empty' objects whilst run
    // gets on with compacting using the returned values.
    return {std::move(lockedState->config),
            std::move(lockedState->cookiesWaiting)};
}

CompactionConfig CompactTask::runCompactionWithConfig(
        std::optional<CompactionConfig> config, const CookieIface* cookie) {
    auto lockedState = compaction.wlock();
    if (config) {
        lockedState->config.merge(config.value());
    }
    if (cookie) {
        lockedState->cookiesWaiting.push_back(cookie);
    }
    lockedState->rescheduleRequired = true;
    return lockedState->config;
}

bool StatSnap::run() {
    TRACE_EVENT0("ep-engine/task", "StatSnap");
    engine->getKVBucket()->snapshotStats(false /*shuttingDown*/);
    ExecutorPool::get()->snooze(uid, 60);
    return true;
}

MultiBGFetcherTask::MultiBGFetcherTask(EventuallyPersistentEngine* e,
                                       BgFetcher* b)
    : GlobalTask(e,
                 TaskId::MultiBGFetcherTask,
                 /*sleeptime*/ INT_MAX,
                 /*completeBeforeShutdown*/ false),
      bgfetcher(b) {
}

bool MultiBGFetcherTask::run() {
    TRACE_EVENT0("ep-engine/task", "MultiBGFetcherTask");
    return bgfetcher->run(this);
}

bool VKeyStatBGFetchTask::run() {
    TRACE_EVENT2("ep-engine/task",
                 "VKeyStatBGFetchTask",
                 "cookie",
                 cookie,
                 "vb",
                 vbucket.get());
    engine->getKVBucket()->completeStatsVKey(cookie, key, vbucket, bySeqNum);
    return false;
}


WorkLoadMonitor::WorkLoadMonitor(EventuallyPersistentEngine *e,
                                 bool completeBeforeShutdown) :
    GlobalTask(e, TaskId::WorkLoadMonitor, WORKLOAD_MONITOR_FREQ,
               completeBeforeShutdown) {
    prevNumMutations = getNumMutations();
    prevNumGets = getNumGets();
}

size_t WorkLoadMonitor::getNumMutations() {
    return engine->getEpStats().numOpsStore +
           engine->getEpStats().numOpsDelete +
           engine->getEpStats().numOpsSetMeta +
           engine->getEpStats().numOpsDelMeta +
           engine->getEpStats().numOpsSetRetMeta +
           engine->getEpStats().numOpsDelRetMeta;
}

size_t WorkLoadMonitor::getNumGets() {
    return engine->getEpStats().numOpsGet +
           engine->getEpStats().numOpsGetMeta;
}

bool WorkLoadMonitor::run() {
    size_t curr_num_mutations = getNumMutations();
    size_t curr_num_gets = getNumGets();
    auto delta_mutations = static_cast<double>(curr_num_mutations -
                                                 prevNumMutations);
    auto delta_gets = static_cast<double>(curr_num_gets - prevNumGets);
    double total_delta_ops = delta_gets + delta_mutations;
    double read_ratio = 0;

    if (total_delta_ops) {
        read_ratio = delta_gets / total_delta_ops;
        if (read_ratio < 0.4) {
            engine->getWorkLoadPolicy().setWorkLoadPattern(WRITE_HEAVY);
        } else if (read_ratio >= 0.4 && read_ratio <= 0.6) {
            engine->getWorkLoadPolicy().setWorkLoadPattern(MIXED);
        } else {
            engine->getWorkLoadPolicy().setWorkLoadPattern(READ_HEAVY);
        }
    }
    prevNumMutations = curr_num_mutations;
    prevNumGets = curr_num_gets;

    snooze(WORKLOAD_MONITOR_FREQ);
    if (engine->getEpStats().isShutdown) {
        return false;
    }
    return true;
}
