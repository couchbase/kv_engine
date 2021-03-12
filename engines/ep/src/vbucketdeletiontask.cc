/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2017 Couchbase, Inc
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

#include "vbucketdeletiontask.h"
#include "ep_engine.h"
#include "ep_vb.h"
#include "executorpool.h"
#include "kvshard.h"
#include "kvstore.h"
#include <phosphor/phosphor.h>
#include <platform/histogram.h>
#include <chrono>

VBucketMemoryDeletionTask::VBucketMemoryDeletionTask(
        EventuallyPersistentEngine& eng, VBucket* vb, TaskId tid)
    : GlobalTask(&eng, tid, 0.0, true), vbucket(vb) {
    if (!vbucket) {
        throw std::logic_error(
                "VBucketMemoryDeletionTask::VBucketMemoryDeletionTask no "
                "vbucket");
    }
    description =
            "Removing (dead) " + vbucket->getId().to_string() + " from memory";
}

VBucketMemoryDeletionTask::~VBucketMemoryDeletionTask() {
    // This task will free the VBucket, which may result in Items being freed
    // For safety, set the engine pointer in the ObjectRegistry (MB-32579)
    auto* e = ObjectRegistry::onSwitchThread(engine, true);
    {
        vbucket.reset();

        // Best effort force the description to deallocate, swap with an empty
        std::string empty;
        description.swap(empty);
    }
    ObjectRegistry::onSwitchThread(e);
}

std::string VBucketMemoryDeletionTask::getDescription() const {
    return description;
}

std::chrono::microseconds VBucketMemoryDeletionTask::maxExpectedDuration()
        const {
    // p99.9 typically around 50ms.
    return std::chrono::milliseconds(100);
}

bool VBucketMemoryDeletionTask::run() {
    TRACE_EVENT1("ep-engine/task",
                 "VBucketMemoryDeletionTask",
                 "vb",
                 (vbucket->getId()).get());

    notifyAllPendingConnsFailed(true);

    return false;
}

void VBucketMemoryDeletionTask::notifyAllPendingConnsFailed(
        bool notifyIfCookieSet) {
    vbucket->notifyAllPendingConnsFailed(*engine);

    if (notifyIfCookieSet && vbucket->getDeferredDeletionCookie()) {
        engine->notifyIOComplete(vbucket->getDeferredDeletionCookie(),
                                 cb::engine_errc::success);
    }
}

VBucketMemoryAndDiskDeletionTask::VBucketMemoryAndDiskDeletionTask(
        EventuallyPersistentEngine& eng, KVShard& shard, EPVBucket* vb)
    : VBucketMemoryDeletionTask(eng,
                                static_cast<VBucket*>(vb),
                                TaskId::VBucketMemoryAndDiskDeletionTask),
      shard(shard),
      vbDeleteRevision(vb->getDeferredDeletionFileRevision()) {
    description += " and disk";
}

bool VBucketMemoryAndDiskDeletionTask::run() {
    TRACE_EVENT1("ep-engine/task",
                 "VBucketMemoryAndDiskDeletionTask",
                 "vb",
                 (vbucket->getId()).get());
    notifyAllPendingConnsFailed(false);

    auto start = std::chrono::steady_clock::now();
    shard.getRWUnderlying()->delVBucket(vbucket->getId(), vbDeleteRevision);
    auto elapsed = std::chrono::steady_clock::now() - start;
    auto wallTime =
            std::chrono::duration_cast<std::chrono::microseconds>(elapsed);

    engine->getEpStats().vbucketDeletions++;
    BlockTimer::log(elapsed, "disk_vb_del", engine->getEpStats().timingLog);
    engine->getEpStats().diskVBDelHisto.add(wallTime);
    atomic_setIfBigger(engine->getEpStats().vbucketDelMaxWalltime,
                       hrtime_t(wallTime.count()));
    engine->getEpStats().vbucketDelTotWalltime.fetch_add(wallTime.count());

    if (vbucket->getDeferredDeletionCookie()) {
        engine->notifyIOComplete(vbucket->getDeferredDeletionCookie(),
                                 cb::engine_errc::success);
    }

    return false;
}
