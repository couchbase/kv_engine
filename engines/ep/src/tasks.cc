/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2013 Couchbase, Inc.
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
#include "tasks.h"
#include "bgfetcher.h"
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
                         const CompactionConfig& c,
                         uint64_t purgeSeqno,
                         const void* ck,
                         bool completeBeforeShutdown)
    : GlobalTask(&bucket.getEPEngine(),
                 TaskId::CompactVBucketTask,
                 0,
                 completeBeforeShutdown),
      bucket(bucket),
      vbid(vbid),
      compactionConfig(c),
      purgeSeqno(purgeSeqno),
      cookie(ck) {
}

bool CompactTask::run() {
    TRACE_EVENT1("ep-engine/task", "CompactTask", "file_id", vbid.get());

    /**
     * MB-30015: Check to see if tombstones that have invalid
     * data needs to be retained. The goal is to try and retain
     * the erroneous tombstones especially in customer environments
     * for further analysis
     */
    compactionConfig.retain_erroneous_tombstones =
                             bucket.isRetainErroneousTombstones();
    return bucket.doCompact(vbid, compactionConfig, purgeSeqno, cookie);
}

std::string CompactTask::getDescription() {
    return "Compact DB file " + std::to_string(vbid.get());
}

bool StatSnap::run() {
    TRACE_EVENT0("ep-engine/task", "StatSnap");
    engine->getKVBucket()->snapshotStats();
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
