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

#include <climits>

#include <type_traits>

static const double VBSTATE_SNAPSHOT_FREQ(300.0);
static const double WORKLOAD_MONITOR_FREQ(5.0);

GlobalTask::GlobalTask(Taskable& t,
                       TaskId taskId,
                       double sleeptime,
                       bool completeBeforeShutdown)
      : RCValue(),
        blockShutdown(completeBeforeShutdown),
        state(TASK_RUNNING),
        uid(nextTaskId()),
        typeId(taskId),
        engine(NULL),
        taskable(t) {
    priority = getTaskPriority(taskId);
    snooze(sleeptime);
}

GlobalTask::GlobalTask(EventuallyPersistentEngine *e,
                       TaskId taskId,
                       double sleeptime,
                       bool completeBeforeShutdown)
      : GlobalTask(e->getTaskable(),
                   taskId,
                   sleeptime,
                   completeBeforeShutdown) {
    engine = e;
}

void GlobalTask::snooze(const double secs) {
    if (secs == INT_MAX) {
        setState(TASK_SNOOZED, TASK_RUNNING);
        updateWaketime(hrtime_t(-1));
        return;
    }

    hrtime_t curTime = gethrtime();
    if (secs) {
        setState(TASK_SNOOZED, TASK_RUNNING);
        waketime.store(curTime + hrtime_t(secs * 1000000000));
    } else {
        waketime.store(curTime);
    }
}

// These static_asserts previously were in priority_test.cc
static_assert(TaskPriority::MultiBGFetcherTask < TaskPriority::BGFetchCallback,
              "MultiBGFetcherTask not less than BGFetchCallback");

static_assert(TaskPriority::BGFetchCallback == TaskPriority::VBDeleteTask,
              "BGFetchCallback not equal VBDeleteTask");

static_assert(TaskPriority::VBStatePersistTaskHigh <
              TaskPriority::VKeyStatBGFetchTask,
              "VBStatePersistTaskHigh not less than VKeyStatBGFetchTask");

static_assert(TaskPriority::VKeyStatBGFetchTask < TaskPriority::FlusherTask,
              "VKeyStatBGFetchTask not less than FlusherTask");

static_assert(TaskPriority::FlusherTask < TaskPriority::ItemPager,
              "FlusherTask not less than ItemPager");

static_assert(TaskPriority::ItemPager < TaskPriority::BackfillManagerTask,
              "ItemPager not less than BackfillManagerTask");

/*
 * Generate a switch statement from tasks.def.h that maps TaskId to a
 * stringified value of the task's name.
 */
const char* GlobalTask::getTaskName(TaskId id) {
    switch(id) {
#define TASK(name, prio) case TaskId::name: {return #name;}
#include "tasks.def.h"
#undef TASK
        case TaskId::TASK_COUNT: {
            throw std::invalid_argument("GlobalTask::getTaskName(TaskId::TASK_COUNT) called.");
        }
    }
    throw std::logic_error("GlobalTask::getTaskName() unknown id " +
                          std::to_string(static_cast<int>(id)));
    return nullptr;
}

/*
 * Generate a switch statement from tasks.def.h that maps TaskId to priority
 */
TaskPriority GlobalTask::getTaskPriority(TaskId id) {
   switch(id) {
#define TASK(name, prio) case TaskId::name: {return TaskPriority::name;}
#include "tasks.def.h"
#undef TASK
        case TaskId::TASK_COUNT: {
            throw std::invalid_argument("GlobalTask::getTaskPriority(TaskId::TASK_COUNT) called.");
        }
    }
    throw std::logic_error("GlobalTask::getTaskPriority() unknown id " +
                           std::to_string(static_cast<int>(id)));
    return TaskPriority::PRIORITY_COUNT;
}

std::array<TaskId, static_cast<int>(TaskId::TASK_COUNT)> GlobalTask::allTaskIds = {{
#define TASK(name, prio) TaskId::name,
#include "tasks.def.h"
#undef TASK
}};


bool FlusherTask::run() {
    return flusher->step(this);
}

bool VBSnapshotTask::run() {
    engine->getEpStore()->snapshotVBuckets(priority, shardID);
    return false;
}

DaemonVBSnapshotTask::DaemonVBSnapshotTask(EventuallyPersistentEngine *e,
                                           bool completeBeforeShutdown)
    : GlobalTask(e, TaskId::DaemonVBSnapshotTask,
                 VBSTATE_SNAPSHOT_FREQ, completeBeforeShutdown) {
    desc = "Snapshotting vbucket states";
}

bool DaemonVBSnapshotTask::run() {
    bool ret = engine->getEpStore()->scheduleVBSnapshot(VBSnapshotTask::Priority::LOW);
    snooze(VBSTATE_SNAPSHOT_FREQ);
    return ret;
}

bool VBStatePersistTask::run() {
    return engine->getEpStore()->persistVBState(vbid);
}

bool VBDeleteTask::run() {
    return !engine->getEpStore()->completeVBucketDeletion(vbucketId, cookie);
}

bool CompactTask::run() {
    return engine->getEpStore()->doCompact(&compactCtx, cookie);
}

bool StatSnap::run() {
    engine->getEpStore()->snapshotStats();
    if (runOnce) {
        return false;
    }
    ExecutorPool::get()->snooze(uid, 60);
    return true;
}

bool MultiBGFetcherTask::run() {
    return bgfetcher->run(this);
}

bool FlushAllTask::run() {
    engine->getEpStore()->reset();
    return false;
}

bool VKeyStatBGFetchTask::run() {
    engine->getEpStore()->completeStatsVKey(cookie, key, vbucket, bySeqNum);
    return false;
}


bool SingleBGFetcherTask::run() {
    engine->getEpStore()->completeBGFetch(key, vbucket, cookie, init,
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
