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
      diskQueueSize(0),
      flusher_todo(0),
      flusherCommits(0),
      cumulativeFlushTime(0),
      cumulativeCommitTime(0),
      totalPersisted(0),
      totalEnqueued(0),
      flushFailed(0),
      flushExpired(0),
      expired_access(0),
      expired_compactor(0),
      expired_pager(0),
      beginFailed(0),
      commitFailed(0),
      vbucketDeletions(0),
      vbucketDeletionFail(0),
      mem_low_wat(0),
      mem_low_wat_percent(0),
      mem_high_wat(0),
      mem_high_wat_percent(0),
      memFreedByCheckpointItemExpel(0),
      forceShutdown(false),
      pendingCompactions(0),
      bg_meta_fetched(0),
      numRemainingBgItems(0),
      numRemainingBgJobs(0),
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
      alogNumItems(0),
      alogTime(0),
      alogRuntime(0),
      expPagerTime(0),
      isShutdown(false),
      rollbackCount(0),
      defragStoredValueNumMoved(0),
      dirtyAgeHisto(),
      diskCommitHisto(),
      timingLog(nullptr),
      maxDataSize(DEFAULT_MAX_DATA_SIZE) {
    EPStats::reset(); // Initialize all remaining stats to their default values
    trackCollectionStats(CollectionID::Default);
}

EPStats::~EPStats() = default;

static_assert(sizeof(EPStats) == 1632,
              "EPStats size is unexpected - have you added/removed stats?");

void EPStats::setMaxDataSize(size_t size) {
    if (size > 0) {
        maxDataSize.store(size);
    }
}

bool EPStats::isMemoryTrackingEnabled() {
    return cb::ArenaMalloc::canTrackAllocations() && GlobalNewDeleteIsOurs;
}

size_t EPStats::getEstimatedTotalMemoryUsed() const {
    if (isMemoryTrackingEnabled()) {
        return cb::ArenaMalloc::getEstimatedAllocated(arena);
    }
    return size_t(std::max(size_t(0), getCurrentSize() + getMemOverhead()));
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

size_t EPStats::getCheckpointManagerEstimatedMemUsage() const {
    int64_t result = 0;
    for (const auto& core : coreLocal) {
        result += core->checkpointManagerEstimatedMemUsage;
    }
    return std::max(int64_t(0), result);
}

size_t EPStats::getNumCheckpoints() const {
    int64_t result = 0;
    for (const auto& core : coreLocal) {
        result += core->numCheckpoints;
    }
    return std::max(int64_t(0), result);
}

size_t EPStats::getCollectionMemUsed(CollectionID cid) const {
    size_t result = 0;
    for (const auto& core : coreLocal) {
        auto collectionMemUsed = core->collectionMemUsed.lock();
        auto itr = collectionMemUsed->find(cid);
        if (itr != collectionMemUsed->end()) {
            result += itr->second;
        }
    }
    return result;
}

std::unordered_map<CollectionID, size_t> EPStats::getAllCollectionsMemUsed()
        const {
    std::unordered_map<CollectionID, size_t> result;
    for (const auto& core : coreLocal) {
        auto collectionMemUsed = core->collectionMemUsed.lock();
        for (auto& pair : *collectionMemUsed) {
            result[pair.first] += pair.second;
        }
    }
    return result;
}

void EPStats::trackCollectionStats(CollectionID cid) {
    for (auto& core : coreLocal) {
        core->collectionMemUsed.lock()->emplace(cid, 0);
    }
}

void EPStats::dropCollectionStats(CollectionID cid) {
    for (auto& core : coreLocal) {
        core->collectionMemUsed.lock()->erase(cid);
    }
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
    tooYoung.reset();
    tooOld.reset();
    totalPersistVBState.reset();
    commit_time.store(0);
    cursorsDropped.reset();
    memFreedByCheckpointRemoval.reset();
    pagerRuns.reset();
    expiryPagerRuns.reset();
    freqDecayerRuns.reset();
    itemsExpelledFromCheckpoints.reset();
    itemsRemovedFromCheckpoints.reset();
    numValueEjects.reset();
    numFailedEjects.reset();
    numNotMyVBuckets.reset();
    bg_fetched.reset();
    bgNumOperations.reset();
    bgWait.store(0);
    bgLoad.store(0);
    oom_errors.reset();
    tmp_oom_errors.reset();
    pendingOps.reset();
    pendingOpsTotal.reset();
    pendingOpsMax.store(0);
    pendingOpsMaxDuration.store(0);
    vbucketDelMaxWalltime.store(0);
    vbucketDelTotWalltime.store(0);
    alogRuns.reset();
    accessScannerSkips.reset();
    defragNumVisited.reset();
    defragNumMoved.reset();
    compressorNumVisited.reset();
    compressorNumCompressed.reset();

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
    seqnoPersistenceHisto.reset();
    diskInsertHisto.reset();
    diskUpdateHisto.reset();
    diskDelHisto.reset();
    diskVBDelHisto.reset();
    diskCommitHisto.reset();
    itemAllocSizeHisto.reset();
    getMultiBatchSizeHisto.reset();
    dirtyAgeHisto.reset();
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
           seqnoPersistenceHisto.getMemFootPrint() +
           diskInsertHisto.getMemFootPrint() +
           diskUpdateHisto.getMemFootPrint() + diskDelHisto.getMemFootPrint() +
           diskVBDelHisto.getMemFootPrint() +
           diskCommitHisto.getMemFootPrint() +
           itemAllocSizeHisto.getMemFootPrint() +
           getMultiBatchSizeHisto.getMemFootPrint() +
           dirtyAgeHisto.getMemFootPrint() +
           persistenceCursorGetItemsHisto.getMemFootPrint() +
           dcpCursorsGetItemsHisto.getMemFootPrint() +
           activeOrPendingFrequencyValuesEvictedHisto.getMemFootPrint() +
           replicaFrequencyValuesEvictedHisto.getMemFootPrint() +
           activeOrPendingFrequencyValuesSnapshotHisto.getMemFootPrint() +
           replicaFrequencyValuesSnapshotHisto.getMemFootPrint() +
           taskHistogramSizes;
}
