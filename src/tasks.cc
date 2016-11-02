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
#include "config.h"

#include "bgfetcher.h"
#include "ep_engine.h"
#include "flusher.h"
#include "tasks.h"
#include "warmup.h"
#include "ep_engine.h"
#include "kvstore.h"

#include <climits>
#include <type_traits>

#include <phosphor/phosphor.h>

static const double WORKLOAD_MONITOR_FREQ(5.0);

bool FlusherTask::run() {
    TRACE_EVENT0("ep-engine/task", "FlusherTask");
    return flusher->step(this);
}

bool VBDeleteTask::run() {
    TRACE_EVENT("ep-engine/task", "VBDeleteTask", vbucketId, cookie);
    return !engine->getKVBucket()->completeVBucketDeletion(vbucketId, cookie);
}

bool CompactTask::run() {
    TRACE_EVENT("ep-engine/task", "CompactTask", compactCtx.db_file_id);
    return engine->getKVBucket()->doCompact(&compactCtx, cookie);
}

bool StatSnap::run() {
    TRACE_EVENT0("ep-engine/task", "StatSnap");
    engine->getKVBucket()->snapshotStats();
    if (runOnce) {
        return false;
    }
    ExecutorPool::get()->snooze(uid, 60);
    return true;
}

bool MultiBGFetcherTask::run() {
    TRACE_EVENT0("ep-engine/task", "MultiBGFetcherTask");
    return bgfetcher->run(this);
}

bool FlushAllTask::run() {
    TRACE_EVENT0("ep-engine/task", "FlushAllTask");
    engine->getKVBucket()->reset();
    return false;
}

bool VKeyStatBGFetchTask::run() {
    TRACE_EVENT("ep-engine/task", "VKeyStatBGFetchTask", cookie, vbucket);
    engine->getKVBucket()->completeStatsVKey(cookie, key, vbucket, bySeqNum);
    return false;
}


bool SingleBGFetcherTask::run() {
    TRACE_EVENT("ep-engine/task", "SingleBGFetcherTask", cookie, vbucket);
    engine->getKVBucket()->completeBGFetch(key, vbucket, cookie, init,
                                           metaFetch);
    return false;
}

WorkLoadMonitor::WorkLoadMonitor(EventuallyPersistentEngine *e,
                                 bool completeBeforeShutdown) :
    GlobalTask(e, TaskId::WorkLoadMonitor, WORKLOAD_MONITOR_FREQ,
               completeBeforeShutdown) {
    prevNumMutations = getNumMutations();
    prevNumGets = getNumGets();
    desc = "Monitoring a workload pattern";
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
    double delta_mutations = static_cast<double>(curr_num_mutations -
                                                 prevNumMutations);
    double delta_gets = static_cast<double>(curr_num_gets - prevNumGets);
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
