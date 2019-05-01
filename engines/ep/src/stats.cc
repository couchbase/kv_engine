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

#include "stats.h"

#include "objectregistry.h"

#include <platform/cb_arena_malloc.h>

#ifndef DEFAULT_MAX_DATA_SIZE
/* Something something something ought to be enough for anybody */
#define DEFAULT_MAX_DATA_SIZE (std::numeric_limits<size_t>::max())
#endif

EPStats::EPStats()
    : warmedUpKeys(0),
      warmedUpValues(0),
      warmedUpPrepares(0),
      warmupItemsVisitedWhilstLoadingPrepares(0),
      warmDups(0),
      warmOOM(0),
      warmupMemUsedCap(0),
      warmupNumReadCap(0),
      replicationThrottleWriteQueueCap(0),
      diskQueueSize(0),
      flusher_todo(0),
      flusherCommits(0),
      cumulativeFlushTime(0),
      cumulativeCommitTime(0),
      tooYoung(0),
      tooOld(0),
      totalPersisted(0),
      totalPersistVBState(0),
      totalEnqueued(0),
      flushFailed(0),
      flushExpired(0),
      expired_access(0),
      expired_compactor(0),
      expired_pager(0),
      beginFailed(0),
      commitFailed(0),
      dirtyAge(0),
      dirtyAgeHighWat(0),
      commit_time(0),
      vbucketDeletions(0),
      vbucketDeletionFail(0),
      mem_low_wat(0),
      mem_low_wat_percent(0),
      mem_high_wat(0),
      mem_high_wat_percent(0),
      cursorDroppingLThreshold(0),
      cursorDroppingUThreshold(0),
      cursorsDropped(0),
      cursorMemoryFreed(0),
      pagerRuns(0),
      expiryPagerRuns(0),
      freqDecayerRuns(0),
      itemsExpelledFromCheckpoints(0),
      itemsRemovedFromCheckpoints(0),
      numValueEjects(0),
      numFailedEjects(0),
      numNotMyVBuckets(0),
      estimatedTotalMemory(0),
      forceShutdown(false),
      oom_errors(0),
      tmp_oom_errors(0),
      pendingOps(0),
      pendingOpsTotal(0),
      pendingOpsMax(0),
      pendingOpsMaxDuration(0),
      pendingCompactions(0),
      bg_fetched(0),
      bg_meta_fetched(0),
      numRemainingBgItems(0),
      numRemainingBgJobs(0),
      bgNumOperations(0),
      bgWait(0),
      bgMinWait(0),
      bgMaxWait(0),
      bgLoad(0),
      bgMinLoad(0),
      bgMaxLoad(0),
      vbucketDelMaxWalltime(0),
      vbucketDelTotWalltime(0),
      replicationThrottleThreshold(0),
      numOpsStore(0),
      numOpsDelete(0),
      numOpsGet(0),
      numOpsGetMeta(0),
      numOpsSetMeta(0),
      numOpsDelMeta(0),
      numOpsSetMetaResolutionFailed(0),
      numOpsDelMetaResolutionFailed(0),
      numOpsSetRetMeta(0),
      numOpsDelRetMeta(0),
      numOpsGetMetaOnSetWithMeta(0),
      alogRuns(0),
      accessScannerSkips(0),
      alogNumItems(0),
      alogTime(0),
      alogRuntime(0),
      expPagerTime(0),
      isShutdown(false),
      rollbackCount(0),
      defragNumVisited(0),
      defragNumMoved(0),
      defragStoredValueNumMoved(0),
      compressorNumVisited(0),
      compressorNumCompressed(0),
      dirtyAgeHisto(),
      diskCommitHisto(),
      timingLog(NULL),
      maxDataSize(DEFAULT_MAX_DATA_SIZE) {
}

EPStats::~EPStats() {
    delete timingLog;
}

void EPStats::setMaxDataSize(size_t size) {
    if (size > 0) {
        maxDataSize.store(size);
    }
}

void EPStats::memAllocated(size_t sz) {
    if (isShutdown) {
        return;
    }

    if (0 == sz) {
        return;
    }

    auto& coreMemory = coreLocal.get()->totalMemory;

    // Update the coreMemory and also create a local copy of the old value + sz
    // This value will be used to check the threshold
    auto value = coreMemory.fetch_add(sz) + sz;

    maybeUpdateEstimatedTotalMemUsed(coreMemory, value);
}

void EPStats::memDeallocated(size_t sz) {
    if (isShutdown) {
        return;
    }

    if (0 == sz) {
        return;
    }

    auto& coreMemory = coreLocal.get()->totalMemory;

    // Update the coreMemory and also create a local copy of the old value - sz
    // This value will be used to check the threshold
    auto value = coreMemory.fetch_sub(sz) - sz;

    maybeUpdateEstimatedTotalMemUsed(coreMemory, value);
}

void EPStats::maybeUpdateEstimatedTotalMemUsed(
        cb::RelaxedAtomic<int64_t>& coreMemory, int64_t value) {
    // If this thread succeeds in swapping, this thread updates total
    // @todo: remove this function and callers - it is deadcode
    if (std::abs(value) > 0) {
        // Swap the core's value to 0 and update total with whatever we got
        estimatedTotalMemory->fetch_add(coreMemory.exchange(0));
    }
}

size_t EPStats::getPreciseTotalMemoryUsed() const {
    if (isMemoryTrackingEnabled()) {
        return cb::ArenaMalloc::getPreciseAllocated(arena);
    }
    return size_t(std::max(size_t(0), getCurrentSize() + getMemOverhead()));
}

size_t EPStats::getCurrentSize() const {
    int64_t result = 0;
    for (const auto& core : coreLocal) {
        result += core->currentSize;
    }
    return std::max(int64_t(0), result);
}

size_t EPStats::getNumBlob() const {
    int64_t result = 0;
    for (const auto& core : coreLocal) {
        result += core->numBlob;
    }
    return std::max(int64_t(0), result);
}

size_t EPStats::getBlobOverhead() const {
    int64_t result = 0;
    for (const auto& core : coreLocal) {
        result += core->blobOverhead;
    }
    return std::max(int64_t(0), result);
}

size_t EPStats::getTotalValueSize() const {
    int64_t result = 0;
    for (const auto& core : coreLocal) {
        result += core->totalValueSize;
    }
    return std::max(int64_t(0), result);
}

size_t EPStats::getNumStoredVal() const {
    int64_t result = 0;
    for (const auto& core : coreLocal) {
        result += core->numStoredVal;
    }
    return std::max(int64_t(0), result);
}

size_t EPStats::getStoredValSize() const {
    int64_t result = 0;
    for (const auto& core : coreLocal) {
        result += core->totalStoredValSize;
    }
    return std::max(int64_t(0), result);
}

size_t EPStats::getMemOverhead() const {
    int64_t result = 0;
    for (const auto& core : coreLocal) {
        result += core->memOverhead;
    }
    return std::max(int64_t(0), result);
}

size_t EPStats::getNumItem() const {
    int64_t result = 0;
    for (const auto& core : coreLocal) {
        result += core->numItem;
    }
    return std::max(int64_t(0), result);
}

void EPStats::setLowWaterMark(size_t value) {
    mem_low_wat.store(value);
    mem_low_wat_percent.store((double)(value) / getMaxDataSize());
}

void EPStats::setHighWaterMark(size_t value) {
    mem_high_wat.store(value);
    mem_high_wat_percent.store((double)(value) / getMaxDataSize());
}

void EPStats::reset() {
    tooYoung.store(0);
    tooOld.store(0);
    totalPersistVBState.store(0);
    dirtyAge.store(0);
    dirtyAgeHighWat.store(0);
    commit_time.store(0);
    cursorsDropped.store(0);
    cursorMemoryFreed.store(0);
    pagerRuns.store(0);
    expiryPagerRuns.store(0);
    freqDecayerRuns.store(0);
    itemsExpelledFromCheckpoints.store(0);
    itemsRemovedFromCheckpoints.store(0);
    numValueEjects.store(0);
    numFailedEjects.store(0);
    numNotMyVBuckets.store(0);
    bg_fetched.store(0);
    bgNumOperations.store(0);
    bgWait.store(0);
    bgLoad.store(0);
    bgMinWait.store(999999999);
    bgMaxWait.store(0);
    bgMinLoad.store(999999999);
    bgMaxLoad.store(0);
    oom_errors.store(0);
    tmp_oom_errors.store(0);
    pendingOps.store(0);
    pendingOpsTotal.store(0);
    pendingOpsMax.store(0);
    pendingOpsMaxDuration.store(0);
    vbucketDelMaxWalltime.store(0);
    vbucketDelTotWalltime.store(0);

    alogRuns.store(0);
    accessScannerSkips.store(0), defragNumVisited.store(0),
            defragNumMoved.store(0);

    compressorNumVisited.store(0);
    compressorNumCompressed.store(0);

    pendingOpsHisto.reset();
    bgWaitHisto.reset();
    bgLoadHisto.reset();
    setWithMetaHisto.reset();
    accessScannerHisto.reset();
    checkpointRemoverHisto.reset();
    itemPagerHisto.reset();
    expiryPagerHisto.reset();
    getVbucketCmdHisto.reset();
    setVbucketCmdHisto.reset();
    delVbucketCmdHisto.reset();
    getCmdHisto.reset();
    storeCmdHisto.reset();
    arithCmdHisto.reset();
    notifyIOHisto.reset();
    getStatsCmdHisto.reset();
    chkPersistenceHisto.reset();
    diskInsertHisto.reset();
    diskUpdateHisto.reset();
    diskDelHisto.reset();
    diskVBDelHisto.reset();
    diskCommitHisto.reset();
    itemAllocSizeHisto.reset();
    getMultiBatchSizeHisto.reset();
    dirtyAgeHisto.reset();
    getMultiHisto.reset();
    persistenceCursorGetItemsHisto.reset();
    dcpCursorsGetItemsHisto.reset();

    activeOrPendingFrequencyValuesEvictedHisto.reset();
    replicaFrequencyValuesEvictedHisto.reset();
    activeOrPendingFrequencyValuesSnapshotHisto.reset();
    replicaFrequencyValuesSnapshotHisto.reset();
    for (auto& hist : syncWriteCommitTimes) {
        hist.reset();
    }
}

size_t EPStats::getMemFootPrint() const {
    size_t taskHistogramSizes = 0;

    if (!schedulingHisto.empty()) {
        taskHistogramSizes +=
                schedulingHisto.size() * schedulingHisto[0].getMemFootPrint();
    }
    if (!taskRuntimeHisto.empty()) {
        taskHistogramSizes +=
                taskRuntimeHisto.size() * taskRuntimeHisto[0].getMemFootPrint();
    }

    return pendingOpsHisto.getMemFootPrint() + bgWaitHisto.getMemFootPrint() +
           bgLoadHisto.getMemFootPrint() + setWithMetaHisto.getMemFootPrint() +
           accessScannerHisto.getMemFootPrint() +
           checkpointRemoverHisto.getMemFootPrint() +
           itemPagerHisto.getMemFootPrint() +
           expiryPagerHisto.getMemFootPrint() +
           getVbucketCmdHisto.getMemFootPrint() +
           setVbucketCmdHisto.getMemFootPrint() +
           delVbucketCmdHisto.getMemFootPrint() +
           getCmdHisto.getMemFootPrint() + storeCmdHisto.getMemFootPrint() +
           arithCmdHisto.getMemFootPrint() + notifyIOHisto.getMemFootPrint() +
           getStatsCmdHisto.getMemFootPrint() +
           chkPersistenceHisto.getMemFootPrint() +
           diskInsertHisto.getMemFootPrint() +
           diskUpdateHisto.getMemFootPrint() + diskDelHisto.getMemFootPrint() +
           diskVBDelHisto.getMemFootPrint() +
           diskCommitHisto.getMemFootPrint() +
           itemAllocSizeHisto.getMemFootPrint() +
           getMultiBatchSizeHisto.getMemFootPrint() +
           dirtyAgeHisto.getMemFootPrint() + getMultiHisto.getMemFootPrint() +
           persistenceCursorGetItemsHisto.getMemFootPrint() +
           dcpCursorsGetItemsHisto.getMemFootPrint() +
           activeOrPendingFrequencyValuesEvictedHisto.getMemFootPrint() +
           replicaFrequencyValuesEvictedHisto.getMemFootPrint() +
           activeOrPendingFrequencyValuesSnapshotHisto.getMemFootPrint() +
           replicaFrequencyValuesSnapshotHisto.getMemFootPrint() +
           taskHistogramSizes;
}
