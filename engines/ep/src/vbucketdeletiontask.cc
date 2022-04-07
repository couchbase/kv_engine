/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2017-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "vbucketdeletiontask.h"
#include "ep_engine.h"
#include "ep_vb.h"
#include "kvshard.h"
#include "kvstore/kvstore.h"
#include <executor/executorpool.h>
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
      vbDeleteRevision(vb->takeDeferredDeletionFileRevision()) {
    description += " and disk";
}

bool VBucketMemoryAndDiskDeletionTask::run() {
    TRACE_EVENT1("ep-engine/task",
                 "VBucketMemoryAndDiskDeletionTask",
                 "vb",
                 (vbucket->getId()).get());
    notifyAllPendingConnsFailed(false);

    auto start = std::chrono::steady_clock::now();
    shard.getRWUnderlying()->delVBucket(vbucket->getId(),
                                        std::move(vbDeleteRevision));
    auto elapsed = std::chrono::steady_clock::now() - start;
    auto wallTime =
            std::chrono::duration_cast<std::chrono::microseconds>(elapsed);

    engine->getEpStats().vbucketDeletions++;
    BlockTimer::log(
            elapsed, "disk_vb_del", engine->getEpStats().timingLog.get());
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
