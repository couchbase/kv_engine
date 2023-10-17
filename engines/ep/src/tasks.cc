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
#include "flusher.h"
#include "warmup.h"
#include <executor/executorpool.h>

#include <climits>
#include <type_traits>

#include <phosphor/phosphor.h>

static const double WORKLOAD_MONITOR_FREQ(5.0);

bool FlusherTask::run() {
    return flusher->step(this);
}

CompactTask::CompactTask(
        EPBucket& bucket,
        Vbid vbid,
        CompactionConfig config,
        std::chrono::steady_clock::time_point requestedStartTime,
        CookieIface* ck,
        cb::AwaitableSemaphore& semaphore,
        bool completeBeforeShutdown)
    : EpLimitedConcurrencyTask(bucket.getEPEngine(),
                               TaskId::CompactVBucketTask,
                               semaphore,
                               completeBeforeShutdown),
      bucket(bucket),
      vbid(vbid) {
    auto lockedState = compaction.wlock();

    lockedState->config = config;

    if (ck) {
        lockedState->cookiesWaiting.push_back(ck);
    }

    lockedState->requestedStartTime = requestedStartTime;
}

bool CompactTask::runInner() {
    TRACE_EVENT1("ep-engine/task", "CompactTask", "file_id", vbid.get());

    // try to acquire a token
    auto guard = acquireOrWait();
    if (!guard) {
        // too many tasks are already running. We have already been snoozed
        // (see NotifiableTask::run()) so just return true to be rescheduled.
        return true;
    }
    // token acquired, guard will release it when this scope ends.

    // pull out the config we have been requested to run with and any cookies
    // that maybe waiting.
    auto configAndCookies = preDoCompact();

    if (!configAndCookies) {
        // if we ran now, we wouldn't be respecting the requested compaction
        // delay. Reschedule, preDoCompact has already updated the wake time.
        return true;
    }

    auto [config, cookies] = *configAndCookies;

    runningCallback();

    auto reschedule = bucket.doCompact(vbid, config, cookies);

    // If this compaction has finished its work (doesn't need to be
    // re-scheduled), then clean up the task and see if any other tasks
    // should now be woken up.
    // Note that compaction for this vBucket may have been re-scheduled
    // while it was running (e.g. one or more collections have been dropped
    // and we need to compact to clean them up), hence updateCompactionTasks()
    // may cause us to have to re-run the task.
    if (!reschedule) {
        reschedule = bucket.updateCompactionTasks(vbid);
    }

    // even if compaction completed, we may still need to reschedule if the
    // config was updated by another compaction request while we were running
    reschedule = !isTaskDone(cookies) || reschedule;

    if (reschedule) {
        // run again as soon as possible. If the compaction has a delay set,
        // we will compute the right wake time next time we run in preDoCompact
        // Task was snoozed forever by NotifiableTask::run()
        wakeup();
    }

    return reschedule;
}

std::string CompactTask::getDescription() const {
    return "Compact DB file " + std::to_string(vbid.get());
}

bool CompactTask::isRescheduleRequired() const {
    return compaction.rlock()->rescheduleRequired;
}

bool CompactTask::isTaskDone(const std::vector<CookieIface*>& cookies) {
    auto handle = compaction.wlock();
    bool shouldReschedule = handle->rescheduleRequired;
    handle->rescheduleRequired = false;
    // Any cookies remaining must be retained for a future run
    handle->cookiesWaiting.insert(
            handle->cookiesWaiting.end(), cookies.begin(), cookies.end());
    // task is done if it did not need rescheduling, and does not have
    // any cookies waiting for notification of compaction completion.
    return !shouldReschedule && handle->cookiesWaiting.empty();
}

CompactionConfig CompactTask::getCurrentConfig() const {
    return compaction.rlock()->config;
}

std::optional<std::pair<CompactionConfig, std::vector<CookieIface*>>>
CompactTask::preDoCompact() {
    auto lockedState = compaction.wlock();
    if (lockedState->requestedStartTime > std::chrono::steady_clock::now()) {
        updateWaketime(lockedState->requestedStartTime);
        return {};
    }
    lockedState->rescheduleRequired = false;
    // config and cookiesWaiting are moved out to the task run loop, any
    // subsequent schedule now operates on the 'empty' objects whilst run
    // gets on with compacting using the returned values.
    return {{std::move(lockedState->config),
             std::move(lockedState->cookiesWaiting)}};
}

CompactionConfig CompactTask::runCompactionWithConfig(
        std::optional<CompactionConfig> config,
        CookieIface* cookie,
        std::chrono::steady_clock::time_point requestedStartTime) {
    auto lockedState = compaction.wlock();
    if (config) {
        lockedState->config.merge(config.value());
    }
    if (cookie) {
        lockedState->cookiesWaiting.push_back(cookie);
    }
    lockedState->rescheduleRequired = true;
    // Whatever delay is set by the new request takes priority.
    lockedState->requestedStartTime = requestedStartTime;
    return lockedState->config;
}

std::vector<CookieIface*> CompactTask::takeCookies() {
    std::vector<CookieIface*> ret;
    auto lockedState = compaction.wlock();
    lockedState->cookiesWaiting.swap(ret);

    return ret;
}

MultiBGFetcherTask::MultiBGFetcherTask(EventuallyPersistentEngine& e,
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
                 &cookie,
                 "vb",
                 vbucket.get());
    engine->getKVBucket()->completeStatsVKey(cookie, key, vbucket, bySeqNum);
    return false;
}

WorkLoadMonitor::WorkLoadMonitor(EventuallyPersistentEngine& e,
                                 bool completeBeforeShutdown)
    : GlobalTask(e,
                 TaskId::WorkLoadMonitor,
                 WORKLOAD_MONITOR_FREQ,
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
