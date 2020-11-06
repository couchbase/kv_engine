/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2018 Couchbase, Inc.
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

#include "ep_bucket.h"

#include "bgfetcher.h"
#include "bucket_logger.h"
#include "checkpoint_manager.h"
#include "collections/manager.h"
#include "collections/persist_manifest_task.h"
#include "collections/vbucket_manifest_handles.h"
#include "dcp/dcpconnmap.h"
#include "ep_engine.h"
#include "ep_time.h"
#include "ep_vb.h"
#include "executorpool.h"
#include "failover-table.h"
#include "flusher.h"
#include "item.h"
#include "persistence_callback.h"
#include "replicationthrottle.h"
#include "rollback_result.h"
#include "tasks.h"
#include "vb_commit.h"
#include "vb_visitors.h"
#include "vbucket_state.h"
#include "warmup.h"

#include <platform/timeutils.h>
#include <statistics/cbstat_collector.h>
#include <statistics/collector.h>
#include <statistics/labelled_collector.h>
#include <utilities/hdrhistogram.h>
#include <utilities/logtags.h>

#include <gsl/gsl-lite.hpp>

#include <utility>

/**
 * Callback class used by EpStore, for adding relevant keys
 * to bloomfilter during compaction.
 */
class BloomFilterCallback : public Callback<Vbid&, const DocKey&, bool&> {
public:
    explicit BloomFilterCallback(KVBucket& eps) : store(eps) {
    }

    void callback(Vbid& vbucketId,
                  const DocKey& key,
                  bool& isDeleted) override {
        VBucketPtr vb = store.getVBucket(vbucketId);
        if (vb) {
            /* Check if a temporary filter has been initialized. If not,
             * initialize it. If initialization fails, throw an exception
             * to the caller and let the caller deal with it.
             */
            bool tempFilterInitialized = vb->isTempFilterAvailable();
            if (!tempFilterInitialized) {
                tempFilterInitialized = initTempFilter(vbucketId);
            }

            if (!tempFilterInitialized) {
                throw std::runtime_error(
                        "BloomFilterCallback::callback: Failed "
                        "to initialize temporary filter for " +
                        vbucketId.to_string());
            }

            if (store.getItemEvictionPolicy() == EvictionPolicy::Value) {
                /**
                 * VALUE-ONLY EVICTION POLICY
                 * Consider deleted items only.
                 */
                if (isDeleted) {
                    vb->addToTempFilter(key);
                }
            } else {
                /**
                 * FULL EVICTION POLICY
                 * If vbucket's resident ratio is found to be less than
                 * the residency threshold, consider all items, otherwise
                 * consider deleted and non-resident items only.
                 */
                bool residentRatioLessThanThreshold =
                        vb->isResidentRatioUnderThreshold(
                                store.getBfiltersResidencyThreshold());
                if (residentRatioLessThanThreshold) {
                    vb->addToTempFilter(key);
                } else {
                    if (isDeleted || !store.isMetaDataResident(vb, key)) {
                        vb->addToTempFilter(key);
                    }
                }
            }
        }
    }

private:
    bool initTempFilter(Vbid vbucketId);
    KVBucket& store;
};

bool BloomFilterCallback::initTempFilter(Vbid vbucketId) {
    Configuration& config = store.getEPEngine().getConfiguration();
    VBucketPtr vb = store.getVBucket(vbucketId);
    if (!vb) {
        return false;
    }

    size_t initial_estimation = config.getBfilterKeyCount();
    size_t estimated_count;
    size_t num_deletes = 0;
    try {
        num_deletes = store.getRWUnderlying(vbucketId)->getNumPersistedDeletes(
                vbucketId);
    } catch (std::runtime_error& re) {
        EP_LOG_WARN(
                "BloomFilterCallback::initTempFilter: runtime error while "
                "getting "
                "number of persisted deletes for {} Details: {}",
                vbucketId,
                re.what());
        return false;
    }

    EvictionPolicy eviction_policy = store.getItemEvictionPolicy();
    if (eviction_policy == EvictionPolicy::Value) {
        /**
         * VALUE-ONLY EVICTION POLICY
         * Obtain number of persisted deletes from underlying kvstore.
         * Bloomfilter's estimated_key_count = 1.25 * deletes
         */
        estimated_count = round(1.25 * num_deletes);
    } else {
        /**
         * FULL EVICTION POLICY
         * First determine if the resident ratio of vbucket is less than
         * the threshold from configuration.
         */
        bool residentRatioAlert = vb->isResidentRatioUnderThreshold(
                store.getBfiltersResidencyThreshold());

        /**
         * Based on resident ratio against threshold, estimate count.
         *
         * 1. If resident ratio is greater than the threshold:
         * Obtain number of persisted deletes from underlying kvstore.
         * Obtain number of non-resident-items for vbucket.
         * Bloomfilter's estimated_key_count =
         *                              1.25 * (deletes + non-resident)
         *
         * 2. Otherwise:
         * Obtain number of items for vbucket.
         * Bloomfilter's estimated_key_count =
         *                              1.25 * (num_items)
         */

        if (residentRatioAlert) {
            estimated_count = round(1.25 * vb->getNumItems());
        } else {
            estimated_count =
                    round(1.25 * (num_deletes + vb->getNumNonResidentItems()));
        }
    }

    if (estimated_count < initial_estimation) {
        estimated_count = initial_estimation;
    }

    vb->initTempFilter(estimated_count, config.getBfilterFpProb());

    return true;
}

class ExpiredItemsCallback : public Callback<Item&, time_t&> {
public:
    explicit ExpiredItemsCallback(KVBucket& store) : epstore(store) {
    }

    void callback(Item& it, time_t& startTime) override {
        if (epstore.compactionCanExpireItems()) {
            epstore.deleteExpiredItem(it, startTime, ExpireBy::Compactor);
        }
    }

private:
    KVBucket& epstore;
};

/**
 * Callback for notifying flusher about pending mutations.
 */
class NotifyFlusherCB : public Callback<Vbid> {
public:
    explicit NotifyFlusherCB(KVShard* sh) : shard(sh) {
    }

    void callback(Vbid& vb) override {
        if (shard->getBucket(vb)) {
            shard->getFlusher()->notifyFlushEvent(vb);
        }
    }

private:
    KVShard* shard;
};

class EPBucket::ValueChangedListener : public ::ValueChangedListener {
public:
    explicit ValueChangedListener(EPBucket& bucket) : bucket(bucket) {
    }

    void sizeValueChanged(const std::string& key, size_t value) override {
        if (key == "flusher_total_batch_limit") {
            bucket.setFlusherBatchSplitTrigger(value);
        } else if (key == "alog_sleep_time") {
            bucket.setAccessScannerSleeptime(value, false);
        } else if (key == "alog_task_time") {
            bucket.resetAccessScannerStartTime();
        } else {
            EP_LOG_WARN("Failed to change value for unknown variable, {}", key);
        }
    }

    void booleanValueChanged(const std::string& key, bool value) override {
        if (key == "access_scanner_enabled") {
            if (value) {
                bucket.enableAccessScannerTask();
            } else {
                bucket.disableAccessScannerTask();
            }
        } else if (key == "retain_erroneous_tombstones") {
            bucket.setRetainErroneousTombstones(value);
        } else  {
            EP_LOG_WARN("Failed to change value for unknown variable, {}", key);
        }
    }

private:
    EPBucket& bucket;
};

EPBucket::EPBucket(EventuallyPersistentEngine& theEngine)
    : KVBucket(theEngine) {
    auto& config = engine.getConfiguration();
    const std::string& policy = config.getItemEvictionPolicy();
    if (policy.compare("value_only") == 0) {
        eviction_policy = EvictionPolicy::Value;
    } else {
        eviction_policy = EvictionPolicy::Full;
    }
    replicationThrottle = std::make_unique<ReplicationThrottleEP>(
            engine.getConfiguration(), stats);

    vbMap.enablePersistence(*this);

    // Pre-7.0.0 BgFetchers were a part of KVShard so keep the same default
    // scaling.
    auto configBgFetcherLimit = config.getMaxNumBgfetchers();
    auto bgFetcherLimit = configBgFetcherLimit == 0 ? vbMap.getNumShards()
                                                    : configBgFetcherLimit;

    // Limit BgFetchers by the number of vBuckets as any more would be useless.
    bgFetcherLimit = std::min(bgFetcherLimit, config.getMaxVbuckets());
    for (size_t i = 0; i < bgFetcherLimit; i++) {
        bgFetchers.emplace_back(std::make_unique<BgFetcher>(*this));
    }

    setFlusherBatchSplitTrigger(config.getFlusherTotalBatchLimit());
    config.addValueChangedListener(
            "flusher_total_batch_limit",
            std::make_unique<ValueChangedListener>(*this));

    retainErroneousTombstones = config.isRetainErroneousTombstones();
    config.addValueChangedListener(
            "retain_erroneous_tombstones",
            std::make_unique<ValueChangedListener>(*this));

    initializeWarmupTask();
}

EPBucket::~EPBucket() {
}

bool EPBucket::initialize() {
    KVBucket::initialize();

    startWarmupTask();

    enableItemPager();

    if (!startBgFetcher()) {
        EP_LOG_CRITICAL(
                "EPBucket::initialize: Failed to create and start "
                "bgFetchers");
        return false;
    }
    startFlusher();

    return true;
}

void EPBucket::initializeShards() {
    vbMap.forEachShard([this](KVShard& shard) {
        shard.getRWUnderlying()->setMakeCompactionContextCallback(
                std::bind(&EPBucket::makeCompactionContext,
                          this,
                          std::placeholders::_1,
                          std::placeholders::_2,
                          std::placeholders::_3));
    });
}

std::vector<ExTask> EPBucket::deinitialize() {
    stopFlusher();
    stopBgFetcher();
    stopWarmup();

    auto ret = KVBucket::deinitialize();

    // Now that we've stopped all of our tasks, stop any tasks the storage
    // layer may have created.
    vbMap.forEachShard([](KVShard& shard) {
        shard.getRWUnderlying()->deinitialize();
    });

    return ret;
}

/**
 * @returns true if the item `candidate` can be de-duplicated (skipped) because
 * `lastFlushed` already supercedes it.
 */
static bool canDeDuplicate(Item* lastFlushed, Item& candidate) {
    if (!lastFlushed) {
        // Nothing to de-duplicate against.
        return false;
    }
    if (lastFlushed->getKey() != candidate.getKey()) {
        // Keys differ - cannot de-dupe.
        return false;
    }
    if (lastFlushed->isCommitted() != candidate.isCommitted()) {
        // Committed / pending namespace differs - cannot de-dupe.
        return false;
    }

    // items match - the candidate must have a lower seqno.
    Expects(lastFlushed->getBySeqno() > candidate.getBySeqno());

    // Otherwise - valid to de-dupe.
    return true;
}

EPBucket::FlushResult EPBucket::flushVBucket(Vbid vbid) {
    const auto flushStart = std::chrono::steady_clock::now();

    auto vb = getLockedVBucket(vbid, std::try_to_lock);
    if (!vb.owns_lock()) {
        // Try another bucket if this one is locked to avoid blocking flusher.
        return {MoreAvailable::Yes, 0, WakeCkptRemover::No};
    }

    if (!vb) {
        return {MoreAvailable::No, 0, WakeCkptRemover::No};
    }

    // Obtain the set of items to flush, up to the maximum allowed for
    // a single flush.
    auto toFlush = vb->getItemsToPersist(flusherBatchSplitTrigger);

    // Callback must be initialized at persistence
    Expects(toFlush.flushHandle.get());

    const auto moreAvailable =
            toFlush.moreAvailable ? MoreAvailable::Yes : MoreAvailable::No;

    // The Flusher will wake up the CheckpointRemover if necessary.
    const auto wakeupCheckpointRemover =
            vb->checkpointManager
                            ->isEligibleForCheckpointRemovalAfterPersistence()
                    ? WakeCkptRemover::Yes
                    : WakeCkptRemover::No;

    if (toFlush.items.empty()) {
        // getItemsToPersist() may move the persistence cursor to a new
        // checkpoint, so some pending CheckpointPersistence request could be
        // satisfied now.
        //
        // Note: We do not need to notify SeqnoPersistence request here, as
        //   there is definitely nothing new to notify if we do not flush any
        //   mutation.
        handleCheckpointPersistence(*vb);

        return {moreAvailable, 0, wakeupCheckpointRemover};
    }

    // The range becomes initialised only when an item is flushed
    std::optional<snapshot_range_t> range;
    KVStore* rwUnderlying = getRWUnderlying(vb->getId());

    while (!rwUnderlying->begin(
            std::make_unique<EPTransactionContext>(stats, *vb))) {
        ++stats.beginFailed;
        EP_LOG_WARN(
                "Failed to start a transaction!!! "
                "Retry in 1 sec ...");
        std::this_thread::sleep_for(std::chrono::seconds(1));
    }

    rwUnderlying->optimizeWrites(toFlush.items);

    Item* prev = nullptr;

    // Read the vbucket_state from disk as many values from the
    // in-memory vbucket_state may be ahead of what we are flushing.
    const auto* persistedVbState = rwUnderlying->getVBucketState(vb->getId());

    // The first flush we do populates the cachedVBStates of the KVStore
    // so we may not (if this is the first flush) have a state returned
    // from the KVStore.
    vbucket_state vbstate;
    if (persistedVbState) {
        vbstate = *persistedVbState;
    }

    VB::Commit commitData(vb->getManifest(), vbstate);
    vbucket_state& proposedVBState = commitData.proposedVBState;

    // We need to set a few values from the in-memory state.
    uint64_t maxSeqno = 0;
    uint64_t maxVbStateOpCas = 0;

    auto minSeqno = std::numeric_limits<uint64_t>::max();

    // Stores the number of items added to the flush-batch in KVStore.
    // Note:
    //  - Does not carry any information on whether the flush-batch is
    //    successfully persisted or not
    //  - Does not account set-vbstate items
    size_t flushBatchSize = 0;

    // Set if we process an explicit set-vbstate item, which requires a flush
    // to disk regardless of whether we have any other item to flush or not
    bool mustPersistVBState = false;

    // HCS is optional because we have to update it on disk only if some
    // Commit/Abort SyncWrite is found in the flush-batch. If we're
    // flushing Disk checkpoints then the toFlush value may be
    // supplied. In this case, this should be the HCS received from the
    // Active node and should be greater than or equal to the HCS for
    // any other item in this flush batch. This is required because we
    // send mutations instead of a commits and would not otherwise
    // update the HCS on disk.
    std::optional<uint64_t> hcs;

    // HPS is optional because we have to update it on disk only if a
    // prepare is found in the flush-batch
    // This value is read at warmup to determine what seqno to stop
    // loading prepares at (there will not be any prepares after this
    // point) but cannot be used to initialise a PassiveDM after warmup
    // as this value will advance into snapshots immediately, without
    // the entire snapshot needing to be persisted.
    std::optional<uint64_t> hps;

    // We always maintain the maxVisibleSeqno at the current value
    // and only change it to a higher-seqno when a flush of a visible
    // item is seen. This value must be tracked to provide a correct
    // snapshot range for non-sync write aware consumers during backfill
    // (the snapshot should not end on a prepare or an abort, as these
    // items will not be sent). This value is also used at warmup so
    // that vbuckets can resume with the same visible seqno as before
    // the restart.
    Monotonic<uint64_t> maxVisibleSeqno{proposedVBState.maxVisibleSeqno};

    if (toFlush.maxDeletedRevSeqno) {
        proposedVBState.maxDeletedSeqno = toFlush.maxDeletedRevSeqno.value();
    }

    VBucket::AggregatedFlushStats aggStats;

    // Iterate through items, checking if we (a) can skip persisting,
    // (b) can de-duplicate as the previous key was the same, or (c)
    // actually need to persist.
    // Note: This assumes items have been sorted by key and then by
    // seqno (see optimizeWrites() above) such that duplicate keys are
    // adjacent but with the highest seqno first.
    // Note(2): The de-duplication here is an optimization to save
    // creating and enqueuing multiple set() operations on the
    // underlying KVStore - however the KVStore itself only stores a
    // single value per key, and so even if we don't de-dupe here the
    // KVStore will eventually - just potentialy after unnecessary work.
    for (const auto& item : toFlush.items) {
        if (!item->shouldPersist()) {
            continue;
        }

        const auto op = item->getOperation();
        if ((op == queue_op::commit_sync_write ||
             op == queue_op::abort_sync_write) &&
            toFlush.checkpointType != CheckpointType::Disk) {
            // If we are receiving a disk snapshot then we want to skip
            // the HCS update as we will persist a correct one when we
            // flush the last item. If we were to persist an incorrect
            // HCS then we would have to backtrack the start seqno of
            // our warmup to ensure that we do warmup prepares that may
            // not have been completed if they were completed out of
            // order.
            hcs = std::max(hcs.value_or(0), item->getPrepareSeqno());
        }

        if (item->isVisible() &&
            static_cast<uint64_t>(item->getBySeqno()) > maxVisibleSeqno) {
            maxVisibleSeqno = static_cast<uint64_t>(item->getBySeqno());
        }

        if (op == queue_op::pending_sync_write) {
            Expects(item->getBySeqno() > 0);
            hps = std::max(hps.value_or(0),
                           static_cast<uint64_t>(item->getBySeqno()));
        }

        if (item->isSystemEvent()) {
            commitData.collections.recordSystemEvent(*item);
        }

        if (op == queue_op::set_vbucket_state) {
            // Only process vbstate if it's sequenced higher (by cas).
            // We use the cas instead of the seqno here because a
            // set_vbucket_state does not increment the lastBySeqno in
            // the CheckpointManager when it is created. This means that
            // it is possible to have two set_vbucket_state items that
            // follow one another with the same seqno. The cas will be
            // bumped for every item so it can be used to distinguish
            // which item is the latest and should be flushed.
            if (item->getCas() > maxVbStateOpCas) {
                // Should only bump the stat once for the latest state
                // change that we want to flush
                if (maxVbStateOpCas == 0) {
                    // There is at least a commit to be done, so
                    // increase todo
                    ++stats.flusher_todo;
                }

                maxVbStateOpCas = item->getCas();

                // It could be the case that the set_vbucket_state is
                // alone, i.e. no mutations are being flushed, we must
                // trigger an update of the vbstate, which will always
                // happen when we set this.
                mustPersistVBState = true;

                // Process the Item's value into the transition struct
                proposedVBState.transition.fromItem(*item);
            }
        } else if (!canDeDuplicate(prev, *item)) {
            // This is an item we must persist.
            prev = item.get();
            ++flushBatchSize;

            if (mcbp::datatype::is_xattr(item->getDataType())) {
                proposedVBState.mightContainXattrs = true;
            }

            flushOneDelOrSet(item, vb.getVB());

            maxSeqno = std::max(maxSeqno, (uint64_t)item->getBySeqno());

            // Track the lowest seqno, so we can set the HLC epoch
            minSeqno = std::min(minSeqno, (uint64_t)item->getBySeqno());
            proposedVBState.maxCas =
                    std::max(proposedVBState.maxCas, item->getCas());

            ++stats.flusher_todo;

            if (!range.has_value()) {
                range = snapshot_range_t{
                        proposedVBState.lastSnapStart,
                        toFlush.ranges.empty()
                                ? proposedVBState.lastSnapEnd
                                : toFlush.ranges.back().getEnd()};
            }

            // Is the item the end item of one of the ranges we're
            // flushing? Note all the work here only affects replica VBs
            auto itr = std::find_if(toFlush.ranges.begin(),
                                    toFlush.ranges.end(),
                                    [&item](auto& range) {
                                        return uint64_t(item->getBySeqno()) ==
                                               range.getEnd();
                                    });

            // If this is the end item, we can adjust the start of our
            // flushed range, which would be used for failure purposes.
            // Primarily by bringing the start to be a consistent point
            // allows for promotion to active to set the fail-over table
            // to a consistent point.
            if (itr != toFlush.ranges.end()) {
                // Use std::max as the flusher is not visiting in seqno
                // order.
                range->setStart(
                        std::max(range->getStart(), itr->range.getEnd()));
                // HCS may be weakly monotonic when received via a disk
                // snapshot so we special case this for the disk
                // snapshot instead of relaxing the general constraint.
                if (toFlush.checkpointType == CheckpointType::Disk &&
                    itr->highCompletedSeqno !=
                            proposedVBState.persistedCompletedSeqno) {
                    hcs = itr->highCompletedSeqno;
                }

                // Now that the end of a snapshot has been reached,
                // store the hps tracked by the checkpoint to disk
                if (itr->highPreparedSeqno) {
                    auto newHps =
                            toFlush.checkpointType == CheckpointType::Memory
                                    ? *(itr->highPreparedSeqno)
                                    : itr->getEnd();
                    proposedVBState.highPreparedSeqno =
                            std::max(proposedVBState.highPreparedSeqno, newHps);
                }
            }
        } else {
            // Item is the same key as the previous[1] one - don't need
            // to flush to disk.
            // [1] Previous here really means 'next' - optimizeWrites()
            //     above has actually re-ordered items such that items
            //     with the same key are ordered from high->low seqno.
            //     This means we only write the highest (i.e. newest)
            //     item for a given key, and discard any duplicate,
            //     older items.
        }

        // Register the item for deferred (flush success only) stats update.
        aggStats.accountItem(*item);
    }

    // Just return if nothing to flush
    if (!mustPersistVBState && flushBatchSize == 0) {
        // The persistence cursor may have moved to a new checkpoint, which may
        // satisfy pending checkpoint-persistence requests
        handleCheckpointPersistence(*vb);

        return {moreAvailable, 0, wakeupCheckpointRemover};
    }

    if (proposedVBState.transition.state == vbucket_state_active) {
        if (maxSeqno) {
            range = snapshot_range_t(maxSeqno, maxSeqno);
        }
    }

    // Update VBstate based on the changes we have just made,
    // then tell the rwUnderlying the 'new' state
    // (which will persisted as part of the commit() below).

    // only update the snapshot range if items were flushed, i.e.
    // don't appear to be in a snapshot when you have no data for it
    // We also update the checkpointType here as this should only
    // change with snapshots.
    if (range) {
        proposedVBState.lastSnapStart = range->getStart();
        proposedVBState.lastSnapEnd = range->getEnd();
        proposedVBState.checkpointType = toFlush.checkpointType;
    }

    // Track the lowest seqno written in spock and record it as
    // the HLC epoch, a seqno which we can be sure the value has a
    // HLC CAS.
    proposedVBState.hlcCasEpochSeqno = vb->getHLCEpochSeqno();
    if (proposedVBState.hlcCasEpochSeqno == HlcCasSeqnoUninitialised &&
        minSeqno != std::numeric_limits<uint64_t>::max()) {
        proposedVBState.hlcCasEpochSeqno = minSeqno;

        // @todo MB-37692: Defer this call at flush-success only or reset
        //  the value if flush fails.
        vb->setHLCEpochSeqno(proposedVBState.hlcCasEpochSeqno);
    }

    if (hcs) {
        Expects(hcs > proposedVBState.persistedCompletedSeqno);
        proposedVBState.persistedCompletedSeqno = *hcs;
    }

    if (hps) {
        Expects(hps > proposedVBState.persistedPreparedSeqno);
        proposedVBState.persistedPreparedSeqno = *hps;
    }

    proposedVBState.maxVisibleSeqno = maxVisibleSeqno;

    // Are we flushing only a new vbstate?
    if (mustPersistVBState && (flushBatchSize == 0)) {
        // @todo MB-37920: This call potentially does 2 things:
        //   1) update the cached vbstate
        //   2) persisted the new vbstate
        // The function returns false if the operation fails. But, (1) may
        // succeed and (2) may fail, which makes function to return false.
        // In that case we do not rollback the cached vbstate, which exposes
        // a wrong on-disk state at that point.
        // Also, when we re-attempt to flush a set-vbstate item we may fail
        // again because of the optimization at
        // vbucket_state::needsToBePersisted().
        if (!rwUnderlying->snapshotVBucket(vbid, commitData.proposedVBState)) {
            // @todo: MB-36773, vbstate update is not retried

            // Flush failed, we need to reset the pcursor to the original
            // position. At the next run the flusher will re-attempt by
            // retrieving all the items from the disk queue again.
            toFlush.flushHandle->markFlushFailed();

            return {MoreAvailable::Yes, 0, WakeCkptRemover::No};
        }

        // Update in-memory vbstate
        rwUnderlying->setVBucketState(vbid, commitData.proposedVBState);

        // The new vbstate was the only thing to flush. All done.
        flushSuccessEpilogue(*vb,
                             flushStart,
                             0 /*itemsFlushed*/,
                             aggStats,
                             commitData.collections);

        return {moreAvailable, 0, wakeupCheckpointRemover};
    }

    // The flush-batch must be non-empty by logic at this point.
    Expects(flushBatchSize > 0);

    // Release the memory allocated for vectors in toFlush before we call
    // into KVStore::commit. This reduces memory peaks (every queued_item in
    // toFlush.items is a pointer (8 bytes); also, having a big
    // toFlush.ranges is not likely but may happen).
    //
    // Note:
    //  - std::vector::clear() leaves the capacity of vector unchanged,
    //    so memory is not released.
    //  - we cannot rely on clear() + shrink_to_fit() as the latter is a
    //    non-binding request to reduce capacity() to size(), it depends on
    //    the implementation whether the request is fulfilled.
    {
        const auto itemsToRelease = std::move(toFlush.items);
        const auto rangesToRelease = std::move(toFlush.ranges);
    }

    // Persist the flush-batch.
    const auto flushSuccess = commit(vbid, *rwUnderlying, commitData);

    if (!flushSuccess) {
        // Flush failed, we need to reset the pcursor to the original
        // position. At the next run the flusher will re-attempt by retrieving
        // all the items from the disk queue again.
        toFlush.flushHandle->markFlushFailed();

        return {MoreAvailable::Yes, 0, WakeCkptRemover::No};
    }

    // Note: We want to update the snap-range only if we have flushed at least
    // one item. I.e. don't appear to be in a snap when you have no data for it
    Expects(range.has_value());
    vb->setPersistedSnapshot(*range);

    uint64_t highSeqno = rwUnderlying->getLastPersistedSeqno(vbid);
    if (highSeqno > 0 && highSeqno != vb->getPersistenceSeqno()) {
        vb->setPersistenceSeqno(highSeqno);
    }

    // Notify the local DM that the Flusher has run. Persistence
    // could unblock some pending Prepares in the DM.
    // If it is the case, this call updates the High Prepared Seqno
    // for this node.
    // In the case of a Replica node, that could trigger a SeqnoAck
    // to the Active.
    //
    // Note: This is a NOP if the there's no Prepare queued in DM.
    //     We could notify the DM only if strictly required (i.e.,
    //     only when the Flusher has persisted up to the snap-end
    //     mutation of an in-memory snapshot, see HPS comments in
    //     PassiveDM for details), but that requires further work.
    //     The main problem is that in general a flush-batch does
    //     not coincide with in-memory snapshots (ie, we don't
    //     persist at snapshot boundaries). So, the Flusher could
    //     split a single in-memory snapshot into multiple
    //     flush-batches. That may happen at Replica, e.g.:
    //
    //     1) received snap-marker [1, 2]
    //     2) received 1:PRE
    //     3) flush-batch {1:PRE}
    //     4) received 2:mutation
    //     5) flush-batch {2:mutation}
    //
    //     In theory we need to notify the DM only at step (5) and
    //     only if the the snapshot contains at least 1 Prepare
    //     (which is the case in our example), but the problem is
    //     that the Flusher doesn't know about 1:PRE at step (5).
    //
    //     So, given that here we are executing in a slow bg-thread
    //     (write+sync to disk), then we can just afford to calling
    //     back to the DM unconditionally.
    vb->notifyPersistenceToDurabilityMonitor();

    flushSuccessEpilogue(*vb,
                         flushStart,
                         flushBatchSize /*itemsFlushed*/,
                         aggStats,
                         commitData.collections);

    // Handle Seqno Persistence requests
    vb->notifyHighPriorityRequests(
            engine, vb->getPersistenceSeqno(), HighPriorityVBNotify::Seqno);

    return {moreAvailable, flushBatchSize, wakeupCheckpointRemover};
}

void EPBucket::handleCheckpointPersistence(VBucket& vb) const {
    auto& manager = *vb.checkpointManager;
    manager.itemsPersisted(); // update pCursorPreCkptId
    vb.notifyHighPriorityRequests(engine,
                                  manager.getPersistenceCursorPreChkId(),
                                  HighPriorityVBNotify::ChkPersistence);
}

void EPBucket::flushSuccessEpilogue(
        VBucket& vb,
        const std::chrono::steady_clock::time_point flushStart,
        size_t itemsFlushed,
        const VBucket::AggregatedFlushStats& aggStats,
        Collections::VB::Flush& collectionFlush) {
    // Clear the flag if set (ie, only at vbucket creation)
    if (vb.setBucketCreation(false)) {
        EP_LOG_DEBUG("EPBucket::flushSuccessEpilogue: {} created", vb.getId());
    }

    // Update flush stats
    const auto flushEnd = std::chrono::steady_clock::now();
    const auto transTime =
            std::chrono::duration_cast<std::chrono::milliseconds>(flushEnd -
                                                                  flushStart)
                    .count();
    const auto transTimePerItem =
            itemsFlushed ? static_cast<double>(transTime) / itemsFlushed : 0;
    lastTransTimePerItem.store(transTimePerItem);
    stats.cumulativeFlushTime.fetch_add(transTime);
    stats.flusher_todo.store(0);
    stats.totalPersistVBState++;

    vb.doAggregatedFlushStats(aggStats);

    // By definition, does not need to be called if no flush performed or
    // if flush failed.
    collectionFlush.flushSuccess(vb.getId(), *this);

    // By definition, this function is called after persisting a batch of
    // data, so it can be safely skipped if no flush performed or if flush
    // failed.
    getRWUnderlying(vb.getId())->pendingTasks();

    // The persistence cursor may have moved to a new checkpoint, which may
    // satisfy pending checkpoint-persistence requests
    handleCheckpointPersistence(vb);
}

void EPBucket::setFlusherBatchSplitTrigger(size_t limit) {
    // If limit is lower than the number of shards then we should run with a
    // limit of 1 as a 0 limit could cause us to fail to flush anything.
    flusherBatchSplitTrigger =
            std::max(size_t(1), limit / ExecutorPool::get()->getNumWriters());
}

size_t EPBucket::getFlusherBatchSplitTrigger() {
    return flusherBatchSplitTrigger;
}

bool EPBucket::commit(Vbid vbid, KVStore& kvstore, VB::Commit& commitData) {
    HdrMicroSecBlockTimer timer(
            &stats.diskCommitHisto, "disk_commit", stats.timingLog);
    auto commit_start = std::chrono::steady_clock::now();

    const auto res = kvstore.commit(commitData);
    if (res) {
        ++stats.flusherCommits;
        // Update in-memory vbstate
        kvstore.setVBucketState(vbid, commitData.proposedVBState);
    } else {
        ++stats.commitFailed;
        EP_LOG_WARN("KVBucket::commit: kvstore.commit failed {}", vbid);
    }

    auto commit_end = std::chrono::steady_clock::now();
    auto commit_time = std::chrono::duration_cast<std::chrono::milliseconds>(
                               commit_end - commit_start)
                               .count();
    stats.commit_time.store(commit_time);
    stats.cumulativeCommitTime.fetch_add(commit_time);

    return res;
}

void EPBucket::startFlusher() {
    for (const auto& shard : vbMap.shards) {
        shard->getFlusher()->start();
    }
}

void EPBucket::stopFlusher() {
    for (const auto& shard : vbMap.shards) {
        auto* flusher = shard->getFlusher();
        EP_LOG_INFO(
                "Attempting to stop the flusher for "
                "shard:{}",
                shard->getId());
        bool rv = flusher->stop(stats.forceShutdown);
        if (rv && !stats.forceShutdown) {
            flusher->wait();
        }
    }
}

bool EPBucket::pauseFlusher() {
    bool rv = true;
    for (const auto& shard : vbMap.shards) {
        auto* flusher = shard->getFlusher();
        if (!flusher->pause()) {
            EP_LOG_WARN(
                    "Attempted to pause flusher in state "
                    "[{}], shard = {}",
                    flusher->stateName(),
                    shard->getId());
            rv = false;
        }
    }
    return rv;
}

bool EPBucket::resumeFlusher() {
    bool rv = true;
    for (const auto& shard : vbMap.shards) {
        auto* flusher = shard->getFlusher();
        if (!flusher->resume()) {
            EP_LOG_WARN(
                    "Attempted to resume flusher in state [{}], "
                    "shard = {}",
                    flusher->stateName(),
                    shard->getId());
            rv = false;
        }
    }
    return rv;
}

void EPBucket::wakeUpFlusher() {
    if (stats.diskQueueSize.load() == 0) {
        for (const auto& shard : vbMap.shards) {
            shard->getFlusher()->wake();
        }
    }
}

bool EPBucket::startBgFetcher() {
    for (const auto& bgFetcher : bgFetchers) {
        bgFetcher->start();
    }
    return true;
}

void EPBucket::stopBgFetcher() {
    EP_LOG_INFO("Stopping bg fetchers");

    for (const auto& shard : vbMap.shards) {
        for (const auto vbid : shard->getVBuckets()) {
            VBucketPtr vb = shard->getBucket(vbid);
            if (vb && vb->hasPendingBGFetchItems()) {
                EP_LOG_WARN(
                        "Shutting down engine while there are still pending "
                        "data "
                        "read for shard {} from database storage",
                        shard->getId());
                break;
            }
        }
    }

    for (const auto& bgFetcher : bgFetchers) {
        bgFetcher->stop();
    }
}

ENGINE_ERROR_CODE EPBucket::scheduleCompaction(Vbid vbid,
                                               const CompactionConfig& c,
                                               const void* cookie) {
    ENGINE_ERROR_CODE errCode = checkForDBExistence(vbid);
    if (errCode != ENGINE_SUCCESS) {
        return errCode;
    }

    /* Obtain the vbucket so we can get the previous purge seqno */
    VBucketPtr vb = vbMap.getBucket(vbid);
    if (!vb) {
        return ENGINE_NOT_MY_VBUCKET;
    }

    LockHolder lh(compactionLock);
    ExTask task = std::make_shared<CompactTask>(
            *this, vbid, c, vb->getPurgeSeqno(), cookie);
    compactionTasks.emplace_back(std::make_pair(vbid, task));
    bool snoozed = false;
    if (compactionTasks.size() > 1) {
        if ((stats.diskQueueSize > compactionWriteQueueCap &&
             compactionTasks.size() > (vbMap.getNumShards() / 2)) ||
            engine.getWorkLoadPolicy().getWorkLoadPattern() == READ_HEAVY) {
            // Snooze a new compaction task.
            // We will wake it up when one of the existing compaction tasks is
            // done.
            task->snooze(60);
            snoozed = true;
        }
    }

    ExecutorPool::get()->schedule(task);

    EP_LOG_INFO(
            "Compaction of {}, task:{}, purge_before_ts:{}, "
            "purge_before_seq:{}, "
            "drop_deletes:{}, snoozed:{}, scheduled (awaiting completion).",
            vbid,
            uint64_t(task->getId()),
            c.purge_before_ts,
            c.purge_before_seq,
            c.drop_deletes,
            snoozed);

    return ENGINE_EWOULDBLOCK;
}

ENGINE_ERROR_CODE EPBucket::cancelCompaction(Vbid vbid) {
    LockHolder lh(compactionLock);
    for (const auto& task : compactionTasks) {
        task.second->cancel();
    }
    return ENGINE_SUCCESS;
}


void EPBucket::flushOneDelOrSet(const queued_item& qi, VBucketPtr& vb) {
    if (!vb) {
        --stats.diskQueueSize;
        return;
    }

    int64_t bySeqno = qi->getBySeqno();
    const bool deleted = qi->isDeleted() && !qi->isPending();

    std::chrono::microseconds dirtyAge =
            std::chrono::duration_cast<std::chrono::microseconds>(
                    std::chrono::steady_clock::now() - qi->getQueuedTime());
    stats.dirtyAgeHisto.add(dirtyAge);
    stats.dirtyAge.store(static_cast<rel_time_t>(dirtyAge.count()));
    stats.dirtyAgeHighWat.store(std::max(stats.dirtyAge.load(),
                                         stats.dirtyAgeHighWat.load()));

    KVStore *rwUnderlying = getRWUnderlying(qi->getVBucketId());
    if (!deleted) {
        // TODO: Need to separate disk_insert from disk_update because
        // bySeqno doesn't give us that information.
        HdrMicroSecBlockTimer timer(
                bySeqno == -1 ? &stats.diskInsertHisto : &stats.diskUpdateHisto,
                bySeqno == -1 ? "disk_insert" : "disk_update",
                stats.timingLog);
        if (qi->isSystemEvent()) {
            rwUnderlying->setSystemEvent(qi);
        } else {
            rwUnderlying->set(qi);
        }
    } else {
        HdrMicroSecBlockTimer timer(
                &stats.diskDelHisto, "disk_delete", stats.timingLog);
        if (qi->isSystemEvent()) {
            rwUnderlying->delSystemEvent(qi);
        } else {
            rwUnderlying->del(qi);
        }
    }
}

void EPBucket::dropKey(Vbid vbid,
                       const DiskDocKey& diskKey,
                       int64_t bySeqno,
                       bool isAbort,
                       int64_t highCompletedSeqno) {
    // dropKey is called to remove a key from the in memory structures
    // (HashTable, DurabilityMonitors etc.). We skip calling this for aborts
    // as they don't exist in the HashTable and this allows us to make stricter
    // sanity checks when dropping keys from the DurabilityMonitors.
    if (isAbort) {
        return;
    }

    auto vb = getVBucket(vbid);
    if (!vb) {
        return;
    }

    auto docKey = diskKey.getDocKey();
    if (docKey.isInSystemCollection()) {
        throw std::logic_error("EPBucket::dropKey called for a system key");
    }

    if (diskKey.isPrepared() && bySeqno > highCompletedSeqno) {
        // ... drop it from the DurabilityMonitor
        vb->dropPendingKey(docKey, bySeqno);
    }

    { // collections read lock scope
        // @todo this lock could be removed - fetchValidValue requires it
        // in-case of expiry, however dropKey doesn't generate expired values
        auto cHandle = vb->lockCollections(docKey);

        // ... drop it from the VB (hashtable)
        vb->dropKey(bySeqno, cHandle);
    }
}

std::shared_ptr<CompactionContext> EPBucket::makeCompactionContext(
        Vbid vbid, CompactionConfig& config, uint64_t purgeSeqno) {
    config.purge_before_ts =
            ep_real_time() -
            getEPEngine().getConfiguration().getPersistentMetadataPurgeAge();

    auto ctx = std::make_shared<CompactionContext>(vbid, config, purgeSeqno);

    BloomFilterCBPtr filter(new BloomFilterCallback(*this));
    ctx->bloomFilterCallback = filter;

    ExpiredItemsCBPtr expiry(new ExpiredItemsCallback(*this));
    ctx->expiryCallback = expiry;

    ctx->droppedKeyCb = std::bind(&EPBucket::dropKey,
                                  this,
                                  vbid,
                                  std::placeholders::_1,
                                  std::placeholders::_2,
                                  std::placeholders::_3,
                                  std::placeholders::_4);

    ctx->completionCallback = std::bind(&EPBucket::compactionCompletionCallback,
                                        this,
                                        std::placeholders::_1);

    return ctx;
}

void EPBucket::compactionCompletionCallback(CompactionContext& ctx) {
    auto vb = getVBucket(ctx.vbid);
    if (!vb) {
        return;
    }

    vb->setPurgeSeqno(ctx.max_purged_seq);
    vb->decrNumTotalItems(ctx.stats.collectionsItemsPurged);
}

void EPBucket::compactInternal(Vbid vbid,
                               std::unique_lock<std::mutex>& vbLock,
                               CompactionConfig& config,
                               uint64_t purgeSeqno) {
    auto ctx = makeCompactionContext(vbid, config, purgeSeqno);
    auto* shard = vbMap.getShardByVbId(vbid);
    auto* store = shard->getRWUnderlying();
    bool result = store->compactDB(vbLock, ctx);

    VBucketPtr vb = getVBucket(vbid);
    if (vb) {
        if (getEPEngine().getConfiguration().isBfilterEnabled() && result) {
            vb->swapFilter();
        } else {
            vb->clearFilter();
        }
    }

    EP_LOG_INFO(
            "Compaction of {} done ({}). "
            "purged tombstones:{}, prepares:{}, "
            "collection_items_erased:alive:{},deleted:{}, "
            "size/items/tombstones/purge_seqno pre{{{}, {}, {}, {}}}, "
            "post{{{}, {}, {}, {}}}",
            vbid,
            result ? "ok" : "failed",
            ctx->stats.tombstonesPurged,
            ctx->stats.preparesPurged,
            ctx->stats.collectionsItemsPurged,
            ctx->stats.collectionsDeletedItemsPurged,
            ctx->stats.pre.size,
            ctx->stats.pre.items,
            ctx->stats.pre.deletedItems,
            ctx->stats.pre.purgeSeqno,
            ctx->stats.post.size,
            ctx->stats.post.items,
            ctx->stats.post.deletedItems,
            ctx->stats.post.purgeSeqno);
}

// Running on WriterTask - CompactTask
bool EPBucket::doCompact(Vbid vbid,
                         CompactionConfig& config,
                         uint64_t purgeSeqno,
                         const void* cookie) {
    ENGINE_ERROR_CODE err = ENGINE_SUCCESS;

    auto vb = getLockedVBucket(vbid, std::try_to_lock);
    if (!vb.owns_lock()) {
        // VB currently locked; try again later.
        return true;
    }

    if (vb) {
        compactInternal(vbid, vb.getLock(), config, purgeSeqno);
    } else if (cookie) {
        err = ENGINE_NOT_MY_VBUCKET;
        engine.storeEngineSpecific(cookie, nullptr);
        /**
         * Decrement session counter here, as memcached thread wouldn't
         * visit the engine interface in case of a NOT_MY_VB notification
         */
        engine.decrementSessionCtr();
    }

    updateCompactionTasks(vbid);

    if (cookie) {
        engine.notifyIOComplete(cookie, err);
    }
    --stats.pendingCompactions;
    return false;
}

void EPBucket::updateCompactionTasks(Vbid db_file_id) {
    LockHolder lh(compactionLock);
    bool erased = false, woke = false;
    auto it = compactionTasks.begin();
    while (it != compactionTasks.end()) {
        if ((*it).first == db_file_id) {
            it = compactionTasks.erase(it);
            erased = true;
        } else {
            ExTask& task = (*it).second;
            if (task->getState() == TASK_SNOOZED) {
                ExecutorPool::get()->wake(task->getId());
                woke = true;
            }
            ++it;
        }
        if (erased && woke) {
            break;
        }
    }
}

std::pair<uint64_t, bool> EPBucket::getLastPersistedCheckpointId(Vbid vb) {
    auto vbucket = vbMap.getBucket(vb);
    if (vbucket) {
        return {vbucket->checkpointManager->getPersistenceCursorPreChkId(),
                true};
    } else {
        return {0, true};
    }
}

ENGINE_ERROR_CODE EPBucket::getFileStats(const BucketStatCollector& collector) {
    const auto numShards = vbMap.getNumShards();
    DBFileInfo totalInfo;

    for (uint16_t shardId = 0; shardId < numShards; shardId++) {
        const auto dbInfo =
                getRWUnderlyingByShard(shardId)->getAggrDbFileInfo();
        totalInfo.spaceUsed += dbInfo.spaceUsed;
        totalInfo.fileSize += dbInfo.fileSize;
    }

    using namespace cb::stats;
    collector.addStat(Key::ep_db_data_size, totalInfo.spaceUsed);
    collector.addStat(Key::ep_db_file_size, totalInfo.fileSize);

    return ENGINE_SUCCESS;
}

ENGINE_ERROR_CODE EPBucket::getPerVBucketDiskStats(const void* cookie,
                                                   const AddStatFn& add_stat) {
    class DiskStatVisitor : public VBucketVisitor {
    public:
        DiskStatVisitor(const void* c, AddStatFn a)
            : cookie(c), add_stat(std::move(a)) {
        }

        void visitBucket(const VBucketPtr& vb) override {
            char buf[32];
            Vbid vbid = vb->getId();
            try {
                auto dbInfo =
                        vb->getShard()->getRWUnderlying()->getDbFileInfo(vbid);

                checked_snprintf(
                        buf, sizeof(buf), "vb_%d:data_size", vbid.get());
                add_casted_stat(buf, dbInfo.spaceUsed, add_stat, cookie);
                checked_snprintf(
                        buf, sizeof(buf), "vb_%d:file_size", vbid.get());
                add_casted_stat(buf, dbInfo.fileSize, add_stat, cookie);
            } catch (std::exception& error) {
                EP_LOG_WARN(
                        "DiskStatVisitor::visitBucket: Failed to build stat: "
                        "{}",
                        error.what());
            }
        }

    private:
        const void* cookie;
        AddStatFn add_stat;
    };

    DiskStatVisitor dsv(cookie, add_stat);
    visit(dsv);
    return ENGINE_SUCCESS;
}

VBucketPtr EPBucket::makeVBucket(
        Vbid id,
        vbucket_state_t state,
        KVShard* shard,
        std::unique_ptr<FailoverTable> table,
        NewSeqnoCallback newSeqnoCb,
        std::unique_ptr<Collections::VB::Manifest> manifest,
        vbucket_state_t initState,
        int64_t lastSeqno,
        uint64_t lastSnapStart,
        uint64_t lastSnapEnd,
        uint64_t purgeSeqno,
        uint64_t maxCas,
        int64_t hlcEpochSeqno,
        bool mightContainXattrs,
        const nlohmann::json* replicationTopology,
        uint64_t maxVisibleSeqno) {
    auto flusherCb = std::make_shared<NotifyFlusherCB>(shard);
    // Not using make_shared or allocate_shared
    // 1. make_shared doesn't accept a Deleter
    // 2. allocate_shared has inconsistencies between platforms in calling
    //    alloc.destroy (libc++ doesn't call it)
    return VBucketPtr(new EPVBucket(id,
                                    state,
                                    stats,
                                    engine.getCheckpointConfig(),
                                    shard,
                                    lastSeqno,
                                    lastSnapStart,
                                    lastSnapEnd,
                                    std::move(table),
                                    flusherCb,
                                    std::move(newSeqnoCb),
                                    makeSyncWriteResolvedCB(),
                                    makeSyncWriteCompleteCB(),
                                    makeSeqnoAckCB(),
                                    engine.getConfiguration(),
                                    eviction_policy,
                                    std::move(manifest),
                                    this,
                                    initState,
                                    purgeSeqno,
                                    maxCas,
                                    hlcEpochSeqno,
                                    mightContainXattrs,
                                    replicationTopology,
                                    maxVisibleSeqno),
                      VBucket::DeferredDeleter(engine));
}

ENGINE_ERROR_CODE EPBucket::statsVKey(const DocKey& key,
                                      Vbid vbucket,
                                      const void* cookie) {
    VBucketPtr vb = getVBucket(vbucket);
    if (!vb) {
        return ENGINE_NOT_MY_VBUCKET;
    }

    return vb->statsVKey(key, cookie, engine);
}

void EPBucket::completeStatsVKey(const void* cookie,
                                 const DocKey& key,
                                 Vbid vbid,
                                 uint64_t bySeqNum) {
    GetValue gcb = getROUnderlying(vbid)->get(DiskDocKey{key}, vbid);

    if (eviction_policy == EvictionPolicy::Full) {
        VBucketPtr vb = getVBucket(vbid);
        if (vb) {
            vb->completeStatsVKey(key, gcb);
        }
    }

    if (gcb.getStatus() == ENGINE_SUCCESS) {
        engine.addLookupResult(cookie, std::move(gcb.item));
    } else {
        engine.addLookupResult(cookie, NULL);
    }

    --stats.numRemainingBgJobs;
    engine.notifyIOComplete(cookie, ENGINE_SUCCESS);
}

/**
 * Class that handles the disk callback during the rollback.
 * For each mutation/deletion which was discarded as part of the rollback,
 * the callback() method is invoked with the key of the discarded update.
 * It can then lookup the state of that key using dbHandle (which represents the
 * new, rolled-back file) and correct the in-memory view:
 *
 * a) If the key is not present in the Rollback header then delete it from
 *    the HashTable (if either didn't exist yet, or had previously been
 *    deleted in the Rollback header).
 * b) If the key is present in the Rollback header then replace the in-memory
 *    value with the value from the Rollback header.
 */
class EPDiskRollbackCB : public RollbackCB {
public:
    EPDiskRollbackCB(EventuallyPersistentEngine& e, uint64_t rollbackSeqno)
        : RollbackCB(), engine(e), rollbackSeqno(rollbackSeqno) {
    }

    void callback(GetValue& val) override {
        if (!val.item) {
            throw std::invalid_argument(
                    "EPDiskRollbackCB::callback: val is NULL");
        }
        if (!kvFileHandle) {
            throw std::logic_error(
                    "EPDiskRollbackCB::callback: kvFileHandle is nullptr");
        }

        // Skip system keys, they aren't stored in the hashtable
        if (val.item->getKey().isInSystemCollection()) {
            return;
        }

        EP_LOG_DEBUG("EPDiskRollbackCB: Handling discarded update: {}",
                     *val.item);

        // This is the item in its current state, after the rollback seqno
        // (i.e. the state that we are reverting)
        UniqueItemPtr postRbSeqnoItem(std::move(val.item));

        VBucketPtr vb = engine.getVBucket(postRbSeqnoItem->getVBucketId());

        // Nuke anything in the prepare namespace, we'll do a "warmup" later
        // which will restore everything to the way it should be and this is
        // far easier than dealing with individual states.
        if (postRbSeqnoItem->isPending() || postRbSeqnoItem->isAbort()) {
            // Log any prepares with majority level as they are vulnerable to
            // being "lost" to an active bounce if it comes back up within the
            // failover window. Only log from the rollback seqno as the active
            // will have any that came before this.
            if (postRbSeqnoItem->isPending() &&
                postRbSeqnoItem->getDurabilityReqs().getLevel() ==
                        cb::durability::Level::Majority &&
                postRbSeqnoItem->getBySeqno() >=
                        static_cast<int64_t>(rollbackSeqno)) {
                EP_LOG_INFO(
                        "({}) Rolling back a Majority level prepare with "
                        "key:{} and seqno:{}",
                        vb->getId(),
                        cb::UserData(postRbSeqnoItem->getKey().to_string()),
                        postRbSeqnoItem->getBySeqno());
            }
            removeDeletedDoc(*vb, *postRbSeqnoItem);
            return;
        }

        EP_LOG_DEBUG("EPDiskRollbackCB: Handling post rollback item: {}",
                     *postRbSeqnoItem);

        // The get value of the item before the rollback seqno
        GetValue preRbSeqnoGetValue =
                engine.getKVBucket()
                        ->getROUnderlying(postRbSeqnoItem->getVBucketId())
                        ->getWithHeader(*kvFileHandle,
                                        DiskDocKey{*postRbSeqnoItem},
                                        postRbSeqnoItem->getVBucketId(),
                                        GetMetaOnly::No);

        // This is the item in the state it was before the rollback seqno
        // (i.e. the desired state). null if there was no previous
        // Item.
        UniqueItemPtr preRbSeqnoItem(std::move(preRbSeqnoGetValue.item));

        if (preRbSeqnoGetValue.getStatus() == ENGINE_SUCCESS) {
            EP_LOG_DEBUG(
                    "EPDiskRollbackCB: Item existed pre-rollback; restoring to "
                    "pre-rollback state: {}",
                    *preRbSeqnoItem);

            if (preRbSeqnoItem->isDeleted()) {
                // If the item existed before, but had been deleted, we
                // should delete it now
                removeDeletedDoc(*vb, *postRbSeqnoItem);
            } else {
                // The item existed before and was not deleted, we need to
                // revert the items state to the preRollbackSeqno state
                MutationStatus mtype = vb->setFromInternal(*preRbSeqnoItem);
                switch (mtype) {
                case MutationStatus::NotFound:
                    // NotFound is valid - if the item has been deleted
                    // in-memory, but that was not flushed to disk as of
                    // post-rollback seqno.
                    break;
                case MutationStatus::WasClean:
                    // Item hasn't been modified since it was persisted to disk
                    // as of post-rollback seqno.
                    break;
                case MutationStatus::WasDirty:
                    // Item was modifed since it was persisted to disk - this
                    // is ok because it's just a mutation which has not yet
                    // been persisted to disk as of post-rollback seqno.
                    break;
                case MutationStatus::NoMem:
                    setStatus(ENGINE_ENOMEM);
                    break;
                case MutationStatus::InvalidCas:
                case MutationStatus::IsLocked:
                case MutationStatus::NeedBgFetch:
                case MutationStatus::IsPendingSyncWrite:
                    std::stringstream ss;
                    ss << "EPDiskRollbackCB: Unexpected status:"
                       << to_string(mtype)
                       << " after setFromInternal for item:" << *preRbSeqnoItem;
                    throw std::logic_error(ss.str());
                }

                // If we are rolling back a deletion then we should increment
                // our disk counts. We need to increment the vBucket disk
                // count here too because we're not going to flush this item
                // later
                if (postRbSeqnoItem->isDeleted() &&
                    postRbSeqnoItem->isCommitted()) {
                    vb->incrNumTotalItems();
                    vb->getManifest()
                            .lock(preRbSeqnoItem->getKey())
                            .incrementItemCount();
                }
            }
        } else if (preRbSeqnoGetValue.getStatus() == ENGINE_KEY_ENOENT) {
            EP_LOG_DEBUG(
                    "EPDiskRollbackCB: Item did not exist pre-rollback; "
                    "removing from VB");

            // If the item did not exist before we should delete it now
            removeDeletedDoc(*vb, *postRbSeqnoItem);
        } else {
            EP_LOG_WARN(
                    "EPDiskRollbackCB::callback:Unexpected Error Status: {}",
                    preRbSeqnoGetValue.getStatus());
        }
    }

    /// Remove a deleted-on-disk document from the VBucket's hashtable.
    void removeDeletedDoc(VBucket& vb, const Item& item) {
        if (vb.removeItemFromMemory(item)) {
            setStatus(ENGINE_SUCCESS);
        } else {
            // Document didn't exist in memory - may have been deleted in since
            // the checkpoint.
            setStatus(ENGINE_KEY_ENOENT);
        }

        if (!item.isDeleted() && item.isCommitted()) {
            // Irrespective of if the in-memory delete succeeded; the document
            // doesn't exist on disk; so decrement the item count.
            vb.decrNumTotalItems();
        }
    }

private:
    EventuallyPersistentEngine& engine;

    /// The seqno to which we are rolling back
    uint64_t rollbackSeqno;
};

RollbackResult EPBucket::doRollback(Vbid vbid, uint64_t rollbackSeqno) {
    KVStore* rwUnderlying = vbMap.getShardByVbId(vbid)->getRWUnderlying();
    auto result = rwUnderlying->rollback(
            vbid,
            rollbackSeqno,
            std::make_unique<EPDiskRollbackCB>(engine, rollbackSeqno));
    return result;
}

void EPBucket::rollbackUnpersistedItems(VBucket& vb, int64_t rollbackSeqno) {
    std::vector<queued_item> items;

    // Iterate until we have no more items for the persistence cursor
    CheckpointManager::ItemsForCursor itemsForCursor;
    do {
        itemsForCursor =
                vb.checkpointManager->getNextItemsForPersistence(items);
        // RAII callback, need to trigger it manually here
        itemsForCursor.flushHandle.reset();

        for (const auto& item : items) {
            if (item->getBySeqno() <= rollbackSeqno ||
                item->isCheckPointMetaItem() ||
                item->getKey().isInSystemCollection()) {
                continue;
            }

            // Currently we remove prepares from the HashTable on completion but
            // they may still exist on disk. If we were to reload the prepare
            // from disk, because we have a new unpersisted one, then we would
            // end up in an inconsistent state to pre-rollback. Just remove the
            // prepare from the HashTable. We will "warm up" any incomplete
            // prepares in a later stage of rollback.
            if (item->isPending()) {
                EP_LOG_INFO(
                        "({}) Rolling back an unpersisted {} prepare with "
                        "key:{} and seqno:{}",
                        vb.getId(),
                        to_string(item->getDurabilityReqs().getLevel()),
                        cb::UserData(item->getKey().to_string()),
                        item->getBySeqno());
                vb.removeItemFromMemory(*item);
                continue;
            }

            if (item->isAbort()) {
                EP_LOG_INFO(
                        "({}) Rolling back an unpersisted abort with "
                        "key:{} and seqno:{}",
                        vb.getId(),
                        cb::UserData(item->getKey().to_string()),
                        item->getBySeqno());
                // Aborts are not kept in the hashtable so do not
                // need to be removed.
                continue;
            }

            // Committed items only past this point
            GetValue gcb = getROUnderlying(vb.getId())
                                   ->get(DiskDocKey{*item}, vb.getId());

            if (gcb.getStatus() == ENGINE_SUCCESS) {
                vb.setFromInternal(*gcb.item.get());
            } else {
                vb.removeItemFromMemory(*item);
            }
        }
    } while (itemsForCursor.moreAvailable);
}

// Perform an in-order scan of the seqno index.
// a) For each Prepared item found, add to a map of outstanding Prepares.
// b) For each Committed (via Mutation or Prepare) item, if there's an
//    outstanding Prepare then that prepare has already been Committed,
//    hence remove it from the map.
//
// At the end of the scan, all outstanding Prepared items (which did not
// have a Commit persisted to disk) will be registered with the Durability
// Monitor.
EPBucket::LoadPreparedSyncWritesResult EPBucket::loadPreparedSyncWrites(
        folly::SharedMutex::WriteHolder& vbStateLh, VBucket& vb) {
    /// Disk load callback for scan.
    struct LoadSyncWrites : public StatusCallback<GetValue> {
        LoadSyncWrites(EPVBucket& vb, uint64_t highPreparedSeqno)
            : vb(vb), highPreparedSeqno(highPreparedSeqno) {
        }

        void callback(GetValue& val) override {
            // Abort the scan early if we have passed the HPS as we don't need
            // to load any more prepares.
            if (val.item->getBySeqno() >
                static_cast<int64_t>(highPreparedSeqno)) {
                // ENOMEM may seem like an odd status code to abort the scan but
                // disk backfill to a given seqno also returns ENGINE_ENOMEM
                // when it has received all the seqnos that it cares about to
                // abort the scan.
                setStatus(ENGINE_ENOMEM);
                return;
            }

            itemsVisited++;
            if (val.item->isPending()) {
                // Pending item which was not aborted (deleted). Add to
                // outstanding Prepare map.
                outstandingPrepares.emplace(val.item->getKey(),
                                            std::move(val.item));
                return;
            }

            if (val.item->isCommitted()) {
                // Committed item. _If_ there's an outstanding prepared
                // SyncWrite, remove it (as it has already been committed).
                outstandingPrepares.erase(val.item->getKey());
                return;
            }
        }

        EPVBucket& vb;

        // HPS after which we can abort the scan
        uint64_t highPreparedSeqno = std::numeric_limits<uint64_t>::max();

        // Number of items our callback "visits". Used to validate how many
        // items we look at when loading SyncWrites.
        uint64_t itemsVisited = 0;

        /// Map of Document key -> outstanding (not yet Committed / Aborted)
        /// prepares.
        std::unordered_map<StoredDocKey, std::unique_ptr<Item>>
                outstandingPrepares;
    };

    auto& epVb = dynamic_cast<EPVBucket&>(vb);
    const auto start = std::chrono::steady_clock::now();

    // Get the kvStore. Using the RW store as the rollback code that will call
    // this function will modify vbucket_state that will only be reflected in
    // RW store. For warmup case, we don't allow writes at this point in time
    // anyway.
    auto* kvStore = getRWUnderlyingByShard(epVb.getShard()->getId());

    // Need the HPS/HCS so the DurabilityMonitor can be fully resumed
    auto vbState = kvStore->getVBucketState(epVb.getId());
    if (!vbState) {
        throw std::logic_error("EPBucket::loadPreparedSyncWrites: processing " +
                               epVb.getId().to_string() +
                               ", but found no vbucket_state");
    }

    // Insert all outstanding Prepares into the VBucket (HashTable &
    // DurabilityMonitor).
    std::vector<queued_item> prepares;
    if (vbState->persistedPreparedSeqno == vbState->persistedCompletedSeqno) {
        // We don't need to warm up anything for this vBucket as all of our
        // prepares have been completed, but we do need to create the DM
        // with our vbucket_state.
        epVb.loadOutstandingPrepares(vbStateLh, *vbState, std::move(prepares));
        // No prepares loaded
        return {0, 0};
    }

    // We optimise this step by starting the scan at the seqno following the
    // Persisted Completed Seqno. By definition, all earlier prepares have been
    // completed (Committed or Aborted).
    uint64_t startSeqno = vbState->persistedCompletedSeqno + 1;

    // The seqno up to which we will scan for SyncWrites
    uint64_t endSeqno = vbState->persistedPreparedSeqno;

    // If we are were in the middle of receiving/persisting a Disk snapshot then
    // we cannot solely rely on the PCS and PPS due to de-dupe/out of order
    // commit. We could have our committed item higher than the HPS if we do a
    // normal mutation after a SyncWrite and we have not yet fully persisted the
    // disk snapshot to correct the high completed seqno. In this case, we need
    // to read the rest of the disk snapshot to ensure that we pick up any
    // completions of prepares that we may attempt to warm up.
    //
    // Example:
    //
    // Relica receives Disk snapshot
    // [1:Prepare(a), 2:Prepare(b), 3:Mutation(a)...]
    //
    // After receiving and flushing the mutation at seqno 3, the replica has:
    // HPS - 0 (only moves on snapshot end)
    // HCS - 1 (the DM takes care of this)
    // PPS - 2
    // PCS - 0 (we can only move the PCS correctly at snap-end)
    //
    // If we warmup in this case then we load SyncWrites from seqno 1 to 2. If
    // we do this then we will skip the logical completion at seqno 3 for the
    // prepare at seqno 1. This will cause us to have a completed SyncWrite in
    // the DM when we transition to memory which will block any further
    // SyncWrite completions on this node.
    if (vbState->checkpointType == CheckpointType::Disk &&
        static_cast<uint64_t>(vbState->highSeqno) != vbState->lastSnapEnd) {
        endSeqno = vbState->highSeqno;

        EP_LOG_INFO(
                "EPBucket::loadPreparedSyncWrites: current snapshot is "
                "disk type and incomplete so loading all prepares from: "
                "{} to {}",
                startSeqno,
                endSeqno);
    }

    // Use ALL_ITEMS filter for the scan. NO_DELETES is insufficient
    // because (committed) SyncDeletes manifest as a prepared_sync_write
    // (doc on disk not deleted) followed by a commit_sync_write (which
    // *is* marked as deleted as that's the resulting state).
    // We need to see that Commit, hence ALL_ITEMS.
    const auto docFilter = DocumentFilter::ALL_ITEMS;
    const auto valFilter = getValueFilterForCompressionMode();

    // Don't expect to find anything already in the HashTable, so use
    // NoLookupCallback.
    auto scanCtx = kvStore->initBySeqnoScanContext(
            std::make_unique<LoadSyncWrites>(epVb, endSeqno),
            std::make_unique<NoLookupCallback>(),
            epVb.getId(),
            startSeqno,
            docFilter,
            valFilter,
            SnapshotSource::Head);

    // Storage problems can lead to a null context, kvstore logs details
    if (!scanCtx) {
        EP_LOG_CRITICAL(
                "EPBucket::loadPreparedSyncWrites: scanCtx is null for {}",
                epVb.getId());
        // No prepares loaded
        return {0, 0};
    }

    auto scanResult = kvStore->scan(*scanCtx);

    auto& storageCB = static_cast<LoadSyncWrites&>(scanCtx->getValueCallback());

    // If we abort our scan early due to reaching the HPS then the scan result
    // will be failure but we will have scanned correctly.
    if (storageCB.getStatus() != ENGINE_ENOMEM) {
        Expects(scanResult == scan_success);
    }

    EP_LOG_DEBUG(
            "EPBucket::loadPreparedSyncWrites: Identified {} outstanding "
            "prepared SyncWrites for {} in {}",
            storageCB.outstandingPrepares.size(),
            epVb.getId(),
            cb::time2text(std::chrono::steady_clock::now() - start));

    // Insert all outstanding Prepares into the VBucket (HashTable &
    // DurabilityMonitor).
    prepares.reserve(storageCB.outstandingPrepares.size());
    for (auto& prepare : storageCB.outstandingPrepares) {
        prepares.emplace_back(std::move(prepare.second));
    }
    // Sequence must be sorted by seqno (ascending) for DurabilityMonitor.
    std::sort(
            prepares.begin(), prepares.end(), [](const auto& a, const auto& b) {
                return a->getBySeqno() < b->getBySeqno();
            });

    auto numPrepares = prepares.size();
    epVb.loadOutstandingPrepares(vbStateLh, *vbState, std::move(prepares));
    return {storageCB.itemsVisited, numPrepares};
}

ValueFilter EPBucket::getValueFilterForCompressionMode() {
    auto compressionMode = engine.getCompressionMode();
    if (compressionMode != BucketCompressionMode::Off) {
        return ValueFilter::VALUES_COMPRESSED;
    }

    return ValueFilter::VALUES_DECOMPRESSED;
}

void EPBucket::notifyNewSeqno(const Vbid vbid, const VBNotifyCtx& notifyCtx) {
    if (notifyCtx.notifyFlusher) {
        notifyFlusher(vbid);
    }
    if (notifyCtx.notifyReplication) {
        notifyReplication(vbid, notifyCtx.bySeqno, notifyCtx.syncWrite);
    }
}

Warmup* EPBucket::getWarmup() const {
    return warmupTask.get();
}

bool EPBucket::isWarmingUp() {
    return warmupTask && !warmupTask->isComplete();
}

bool EPBucket::isWarmupOOMFailure() {
    return warmupTask && warmupTask->hasOOMFailure();
}

bool EPBucket::maybeWaitForVBucketWarmup(const void* cookie) {
    if (warmupTask) {
        return warmupTask->maybeWaitForVBucketWarmup(cookie);
    }
    return false;
}

void EPBucket::initializeWarmupTask() {
    if (engine.getConfiguration().isWarmup()) {
        warmupTask = std::make_unique<Warmup>(*this, engine.getConfiguration());
    }
}

void EPBucket::startWarmupTask() {
    if (warmupTask) {
        warmupTask->start();
    } else {
        // No warmup, immediately online the bucket.
        warmupCompleted();
    }
}

bool EPBucket::maybeEnableTraffic() {
    // @todo rename.. skal vaere isTrafficDisabled elns
    auto memoryUsed =
            static_cast<double>(stats.getEstimatedTotalMemoryUsed());
    auto maxSize = static_cast<double>(stats.getMaxDataSize());

    if (memoryUsed >= stats.mem_low_wat) {
        EP_LOG_INFO(
                "Total memory use reached to the low water mark, stop warmup"
                ": memoryUsed ({}) >= low water mark ({})",
                memoryUsed,
                uint64_t(stats.mem_low_wat.load()));
        return true;
    } else if (memoryUsed > (maxSize * stats.warmupMemUsedCap)) {
        EP_LOG_INFO(
                "Enough MB of data loaded to enable traffic"
                ": memoryUsed ({}) > (maxSize({}) * warmupMemUsedCap({}))",
                memoryUsed,
                maxSize,
                stats.warmupMemUsedCap.load());
        return true;
    } else if (eviction_policy == EvictionPolicy::Value &&
               stats.warmedUpValues >=
                       (stats.warmedUpKeys * stats.warmupNumReadCap)) {
        // Let ep-engine think we're done with the warmup phase
        // (we should refactor this into "enableTraffic")
        EP_LOG_INFO(
                "Enough number of items loaded to enable traffic (value "
                "eviction)"
                ": warmedUpValues({}) >= (warmedUpKeys({}) * "
                "warmupNumReadCap({}))",
                uint64_t(stats.warmedUpValues.load()),
                uint64_t(stats.warmedUpKeys.load()),
                stats.warmupNumReadCap.load());
        return true;
    } else if (eviction_policy == EvictionPolicy::Full &&
               stats.warmedUpValues >= (warmupTask->getEstimatedItemCount() *
                                        stats.warmupNumReadCap)) {
        // In case of FULL EVICTION, warmed up keys always matches the number
        // of warmed up values, therefore for honoring the min_item threshold
        // in this scenario, we can consider warmup's estimated item count.
        EP_LOG_INFO(
                "Enough number of items loaded to enable traffic (full "
                "eviction)"
                ": warmedUpValues({}) >= (warmup est items({}) * "
                "warmupNumReadCap({}))",
                uint64_t(stats.warmedUpValues.load()),
                uint64_t(warmupTask->getEstimatedItemCount()),
                stats.warmupNumReadCap.load());
        return true;
    }
    return false;
}

void EPBucket::warmupCompleted() {
    // Snapshot VBucket state after warmup to ensure Failover table is
    // persisted.
    scheduleVBStatePersist();

    if (engine.getConfiguration().getAlogPath().length() > 0) {
        if (engine.getConfiguration().isAccessScannerEnabled()) {
            {
                LockHolder lh(accessScanner.mutex);
                accessScanner.enabled = true;
            }
            EP_LOG_INFO("Access Scanner task enabled");
            size_t smin = engine.getConfiguration().getAlogSleepTime();
            setAccessScannerSleeptime(smin, true);
        } else {
            LockHolder lh(accessScanner.mutex);
            accessScanner.enabled = false;
            EP_LOG_INFO("Access Scanner task disabled");
        }

        Configuration& config = engine.getConfiguration();
        config.addValueChangedListener(
                "access_scanner_enabled",
                std::make_unique<ValueChangedListener>(*this));
        config.addValueChangedListener(
                "alog_sleep_time",
                std::make_unique<ValueChangedListener>(*this));
        config.addValueChangedListener(
                "alog_task_time",
                std::make_unique<ValueChangedListener>(*this));
    }

    ExecutorPool* iom = ExecutorPool::get();
    ExTask task = std::make_shared<StatSnap>(&engine);
    statsSnapshotTaskId = iom->schedule(task);

    // Whilst we do schedule a compaction here and it can run before we call
    // initializeShards below (which sets makeCompactionContext), this scheduled
    // compaction will execute correctly as it takes the compactDB path which
    // will generate a compaction context in EPBucket instead of by calling
    // makeCompactionContext.
    collectionsManager->warmupCompleted(*this);

    // Now warmup is completed, reconfigure each KVStore to use the "proper"
    // makeCompactionContext which allows expiry via compaction, purging
    // collections etc..
    initializeShards();
}

void EPBucket::stopWarmup() {
    // forcefully stop current warmup task
    if (isWarmingUp()) {
        EP_LOG_INFO(
                "Stopping warmup while engine is loading "
                "data from underlying storage, shutdown = {}",
                stats.isShutdown ? "yes" : "no");
        warmupTask->stop();
    }
}

bool EPBucket::isByIdScanSupported() const {
    return getStorageProperties().hasByIdScan();
}

bool EPBucket::isValidBucketDurabilityLevel(cb::durability::Level level) const {
    switch (level) {
    case cb::durability::Level::None:
    case cb::durability::Level::Majority:
    case cb::durability::Level::MajorityAndPersistOnMaster:
    case cb::durability::Level::PersistToMajority:
        return true;
    }
    folly::assume_unreachable();
}

// task does not own the manifest, cookie does
bool EPBucket::maybeScheduleManifestPersistence(
        const void* cookie,
        std::unique_ptr<Collections::Manifest>& newManifest) {
    getEPEngine().storeEngineSpecific(
            cookie, reinterpret_cast<void*>(newManifest.get()));

    ExTask task = std::make_shared<Collections::PersistManifestTask>(
            *this, std::move(newManifest), cookie);
    ExecutorPool::get()->schedule(task);
    return true; // we took the manifest
}

std::ostream& operator<<(std::ostream& os, const EPBucket::FlushResult& res) {
    os << std::boolalpha << "moreAvailable:{"
       << (res.moreAvailable == EPBucket::MoreAvailable::Yes) << "} "
       << "numFlushed:{" << res.numFlushed << "} "
       << "wakeupCkptRemover:{"
       << (res.wakeupCkptRemover == EPBucket::WakeCkptRemover::Yes) << "}";
    return os;
}

BgFetcher& EPBucket::getBgFetcher(Vbid vbid) {
    // For now we just use modulo, same as we do/would for shards to pick out
    // the associated BgFetcher
    auto id = vbid.get() % bgFetchers.size();
    return *bgFetchers.at(id);
}
