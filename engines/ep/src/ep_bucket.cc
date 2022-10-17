/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2018-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
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
#include "failover-table.h"
#include "flusher.h"
#include "item.h"
#include "kvstore/kvstore.h"
#include "kvstore/kvstore_transaction_context.h"
#include "kvstore/persistence_callback.h"
#include "kvstore/rollback_callback.h"
#include "range_scans/range_scan_callbacks.h"
#include "replicationthrottle.h"
#include "rollback_result.h"
#include "tasks.h"
#include "vb_commit.h"
#include "vb_visitors.h"
#include "vbucket_state.h"
#include "warmup.h"

#include <executor/executorpool.h>
#include <fmt/ostream.h>
#include <folly/CancellationToken.h>
#include <folly/synchronization/Baton.h>
#include <hdrhistogram/hdrhistogram.h>
#include <memcached/range_scan_optional_configuration.h>
#include <memcached/server_document_iface.h>
#include <platform/timeutils.h>
#include <statistics/cbstat_collector.h>
#include <statistics/collector.h>
#include <statistics/labelled_collector.h>
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

    void callback(Item& item, time_t& startTime) override {
        epstore.processExpiredItem(item, startTime, ExpireBy::Compactor);
    }

private:
    KVBucket& epstore;
};

void NotifyFlusherCB::callback(Vbid& vbid) {
    auto vb = shard->getBucket(vbid);
    if (vb) {
        vb->getFlusher()->notifyFlushEvent(vb);
    }
}

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

EPBucket::EPBucket(EventuallyPersistentEngine& engine)
    : KVBucket(engine), rangeScans(engine.getConfiguration()) {
    auto& config = engine.getConfiguration();
    const std::string& policy = config.getItemEvictionPolicy();
    if (policy.compare("value_only") == 0) {
        eviction_policy = EvictionPolicy::Value;
    } else {
        eviction_policy = EvictionPolicy::Full;
    }
    replicationThrottle = std::make_unique<ReplicationThrottleEP>(engine);

    // Pre 7.0.0 Flushers were a part of KVShard so keep the same default
    // scaling.
    auto configFlusherLimit = config.getMaxNumFlushers();
    auto flusherLimit =
            configFlusherLimit == 0 ? vbMap.getNumShards() : configFlusherLimit;
    for (size_t i = 0; i < flusherLimit; i++) {
        flushers.emplace_back(std::make_unique<Flusher>(this, i));
    }

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

    // create the semaphore with a default capacity of 1. This will be
    // updated when a compaction is scheduled.
    compactionSemaphore = std::make_unique<cb::AwaitableSemaphore>();

    initializeWarmupTask();
}

EPBucket::~EPBucket() = default;

bool EPBucket::initialize() {
    KVBucket::initialize();

    startWarmupTask();

    enableItemPager();

    if (!startBgFetcher()) {
        EP_LOG_CRITICAL_RAW(
                "EPBucket::initialize: Failed to create and start "
                "bgFetchers");
        return false;
    }

    return true;
}

void EPBucket::initializeShards() {
    vbMap.forEachShard([this](KVShard& shard) {
        shard.getRWUnderlying()->setMakeCompactionContextCallback(
                [this](Vbid vbid,
                       CompactionConfig& config,
                       uint64_t purgeSeqno) {
                    return makeCompactionContext(vbid, config, purgeSeqno);
                });
    });
}

void EPBucket::deinitialize() {
    // If Bucket is currently paused; need to resume to allow flushers
    // etc to complete.
    if (paused) {
        prepareForResume();
    }

    stopFlusher();

    allVbucketsDeinitialize();

    stopBgFetcher();

    KVBucket::deinitialize();

    // Persist the type of shutdown (stats.forceShutdown), and consequently
    // on the next warmup can determine is there was a clean shutdown - see
    // Warmup::cleanShutdown.
    persistShutdownContext();

    // Now that we've stopped all of our tasks, stop any tasks the storage
    // layer may have created.
    vbMap.forEachShard([](KVShard& shard) {
        shard.getRWUnderlying()->deinitialize();
    });
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
    auto vb = getLockedVBucket(vbid, std::try_to_lock);
    if (!vb.owns_lock()) {
        // Try another bucket if this one is locked to avoid blocking flusher.
        return {MoreAvailable::Yes, 0};
    }

    if (!vb) {
        return {MoreAvailable::No, 0};
    }

    return flushVBucket_UNLOCKED(std::move(vb));
}

EPBucket::FlushResult EPBucket::flushVBucket_UNLOCKED(LockedVBucketPtr vb) {
    if (!vb || !vb.owns_lock()) {
        // should never really hit this code, if we do you're using the method
        // incorrectly
        std::logic_error(fmt::format(
                "EPBucket::flushVBucket_UNLOCKED(): should always be called "
                "with a valid LockedVBucketPtr: VbucketPtr:{} owns_lock:{}",
                bool{vb},
                vb.owns_lock()));
    }

    const auto vbid = vb->getId();
    const auto flushStart = std::chrono::steady_clock::now();
    // Obtain the set of items to flush, up to the maximum allowed for
    // a single flush.
    auto toFlush = vb->getItemsToPersist(flusherBatchSplitTrigger);

    // Callback must be initialized at persistence
    Expects(toFlush.flushHandle.get());

    const auto moreAvailable =
            toFlush.moreAvailable ? MoreAvailable::Yes : MoreAvailable::No;

    if (toFlush.items.empty()) {
        return {moreAvailable, 0};
    }

    // The range becomes initialised only when an item is flushed
    std::optional<snapshot_range_t> range;
    auto* rwUnderlying = getRWUnderlying(vb->getId());

    auto ctx = rwUnderlying->begin(
            vbid, std::make_unique<EPPersistenceCallback>(stats, *vb));
    while (!ctx) {
        ++stats.beginFailed;
        EP_LOG_WARN(
                "EPBucket::flushVBucket_UNLOCKED: () Failed to start a "
                "transaction. "
                "Retry in 1 second.",
                vb->getId());
        std::this_thread::sleep_for(std::chrono::seconds(1));
        ctx = rwUnderlying->begin(
                vbid, std::make_unique<EPPersistenceCallback>(stats, *vb));
    }

    bool mustDedupe =
            !rwUnderlying->getStorageProperties().hasAutomaticDeduplication();

    if (mustDedupe) {
        rwUnderlying->prepareForDeduplication(toFlush.items);
    }

    Item* prev = nullptr;

    // Read the vbucket_state from disk as many values from the
    // in-memory vbucket_state may be ahead of what we are flushing.
    const auto* persistedVbState =
            rwUnderlying->getCachedVBucketState(vb->getId());

    // The first flush we do populates the cachedVBStates of the KVStore
    // so we may not (if this is the first flush) have a state returned
    // from the KVStore.
    vbucket_state vbstate;
    if (persistedVbState) {
        vbstate = *persistedVbState;
    }

    // Callback executed at KVStore::commit.
    bool logged = false;
    const auto callback = [this, &logged, vbid](const std::system_error& err) {
        if (!logged) {
            EP_LOG_WARN(
                    "EPBucket::flushVBucket_UNLOCKED: {} {}", vbid, err.what());
            logged = true;
        }

        // MB-42224: sync-header failure callback increments
        // ep_data_write_failed, which is what ns_server uses for
        // detecting a high rate of disk-write failures and failing the
        // node if the user enabled auto-failover.
        ++(this->stats.commitFailed);

        // Return true to let couchstore re-try the operation
        return true;
    };

    WriteOperation writeOp = WriteOperation::Upsert;

    // A disk snapshot has unique items and if we're receiving a vbucket from
    // the start (seqno 0), we can issue Insert operations. This benefits some
    // KVStore implementations since a lookup isn't needed. This scenario
    // commonly occurs in rebalance where a vbucket is taken over by a new node.
    if (toFlush.ranges.size() == 1 &&
        toFlush.checkpointType == CheckpointType::InitialDisk) {
        writeOp = WriteOperation::Insert;
    }

    VB::Commit commitData(vb->getManifest(), writeOp, vbstate, callback);

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

    AggregatedFlushStats aggStats;

    // Iterate through items, checking if we (a) can skip persisting,
    // (b) can de-duplicate as the previous key was the same, or (c)
    // actually need to persist.
    // Note: This assumes items have been sorted by key and then by
    // seqno (see prepareForDeduplication() above) such that duplicate keys are
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
            !isDiskCheckpointType(toFlush.checkpointType)) {
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
        } else if (!mustDedupe || !canDeDuplicate(prev, *item)) {
            // This is an item we must persist.
            prev = item.get();
            ++flushBatchSize;

            if (cb::mcbp::datatype::is_xattr(item->getDataType())) {
                proposedVBState.mightContainXattrs = true;
            }

            flushOneDelOrSet(*ctx, item, vb.getVB());

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
                if (isDiskCheckpointType(toFlush.checkpointType) &&
                    itr->highCompletedSeqno !=
                            proposedVBState.persistedCompletedSeqno) {
                    hcs = itr->highCompletedSeqno;
                }

                // Now that the end of a snapshot has been reached,
                // store the hps tracked by the checkpoint to disk
                switch (toFlush.checkpointType) {
                case CheckpointType::Memory:
                    if (itr->highPreparedSeqno) {
                        proposedVBState.highPreparedSeqno =
                                std::max(proposedVBState.highPreparedSeqno,
                                         *(itr->highPreparedSeqno));
                    }
                    break;
                case CheckpointType::InitialDisk:
                case CheckpointType::Disk:
                    // MB-51639: We need to set the HPS value to the snapshot
                    // end regardless of whether we have a prepare or not
                    // because whilst the active does not complete prepares we
                    // are about to persist their logical completions and so
                    // we can move the HPS.
                    proposedVBState.highPreparedSeqno = std::max(
                            proposedVBState.highPreparedSeqno, itr->getEnd());
                    break;
                }
            }
        } else {
            // Item is the same key as the previous[1] one - don't need
            // to flush to disk.
            // [1] Previous here really means 'next' - prepareForDeduplication()
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
        return {moreAvailable, 0};
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
        if (hcs <= proposedVBState.persistedCompletedSeqno) {
            throw std::logic_error(fmt::format(
                    "EPBucket::flushVBucket_UNLOCKED: {} Trying to set PCS to "
                    "{} but "
                    "the current value is {} and the PCS must be monotonic. "
                    "The current checkpoint type is {}. Flush's seqno "
                    "range:[{},{}], proposedVBState:'{}'.",
                    vbid,
                    *hcs,
                    proposedVBState.persistedCompletedSeqno,
                    to_string(toFlush.checkpointType),
                    minSeqno,
                    maxSeqno,
                    proposedVBState));
        }
        proposedVBState.persistedCompletedSeqno = *hcs;
    }

    if (hps) {
        if (hps <= proposedVBState.persistedPreparedSeqno) {
            throw std::logic_error(fmt::format(
                    "EPBucket::flushVBucket_UNLOCKED: {} Trying to set PPS to "
                    "{} but "
                    "the current value is {} and the PPS must be monotonic. "
                    "The current checkpoint type is {}. Flush's seqno "
                    "range:[{},{}], proposedVBState:'{}'.",
                    vbid,
                    *hps,
                    proposedVBState.persistedPreparedSeqno,
                    to_string(toFlush.checkpointType),
                    minSeqno,
                    maxSeqno,
                    proposedVBState));
        }
        proposedVBState.persistedPreparedSeqno = *hps;
    }

    proposedVBState.maxVisibleSeqno = maxVisibleSeqno;

    // Are we flushing only a new vbstate?
    if (mustPersistVBState && (flushBatchSize == 0)) {
        if (!rwUnderlying->snapshotVBucket(vbid, commitData.proposedVBState)) {
            flushFailureEpilogue(*vb, toFlush);

            return {MoreAvailable::Yes, 0};
        }

        // The new vbstate was the only thing to flush. All done.
        flushSuccessEpilogue(*vb,
                             flushStart,
                             0 /*itemsFlushed*/,
                             aggStats,
                             commitData.collections);

        return {moreAvailable, 0};
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
    if (!commit(*rwUnderlying, std::move(ctx), commitData)) {
        flushFailureEpilogue(*vb, toFlush);

        return {MoreAvailable::Yes, 0};
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
    vb->notifyHighPriorityRequests(engine, vb->getPersistenceSeqno());

    return {moreAvailable, flushBatchSize};
}

void EPBucket::flushSuccessEpilogue(
        VBucket& vb,
        const std::chrono::steady_clock::time_point flushStart,
        size_t itemsFlushed,
        const AggregatedFlushStats& aggStats,
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
    ++stats.flusherCommits;

    vb.doAggregatedFlushStats(aggStats);

    // By definition, does not need to be called if no flush performed or
    // if flush failed.
    collectionFlush.flushSuccess(vb.getId(), *this);

    // By definition, this function is called after persisting a batch of
    // data, so it can be safely skipped if no flush performed or if flush
    // failed.
    getRWUnderlying(vb.getId())->pendingTasks();
}

void EPBucket::flushFailureEpilogue(VBucket& vb, ItemsToFlush& flush) {
    // Flush failed, we need to reset the pcursor to the original
    // position. At the next run the flusher will re-attempt by retrieving
    // all the items from the disk queue again.
    flush.flushHandle->markFlushFailed(vb);

    ++stats.commitFailed;
}

void EPBucket::setFlusherBatchSplitTrigger(size_t limit) {
    // If limit is lower than the number of writers then we should run with a
    // limit of 1 as a 0 limit could cause us to fail to flush anything.
    flusherBatchSplitTrigger =
            std::max(size_t(1), limit / ExecutorPool::get()->getNumWriters());
}

size_t EPBucket::getFlusherBatchSplitTrigger() {
    return flusherBatchSplitTrigger;
}

bool EPBucket::commit(KVStoreIface& kvstore,
                      std::unique_ptr<TransactionContext> txnCtx,
                      VB::Commit& commitData) {
    HdrMicroSecBlockTimer timer(
            &stats.diskCommitHisto, "disk_commit", stats.timingLog.get());
    const auto commit_start = std::chrono::steady_clock::now();

    auto vbid = txnCtx->vbid;
    const auto res = kvstore.commit(std::move(txnCtx), commitData);
    if (!res) {
        EP_LOG_WARN("KVBucket::commit: kvstore.commit failed {}", vbid);
    }

    const auto commit_time =
            std::chrono::duration_cast<std::chrono::milliseconds>(
                    std::chrono::steady_clock::now() - commit_start)
                    .count();
    stats.commit_time.store(commit_time);
    stats.cumulativeCommitTime.fetch_add(commit_time);

    return res;
}

void EPBucket::startFlusher() {
    for (const auto& flusher : flushers) {
        flusher->start();
    }
}

void EPBucket::stopFlusher() {
    for (const auto& flusher : flushers) {
        EP_LOG_INFO("Attempting to stop flusher:{}", flusher->getId());
        bool rv = flusher->stop(stats.forceShutdown);
        if (rv && !stats.forceShutdown) {
            flusher->wait();
        }
    }
}

bool EPBucket::pauseFlusher() {
    bool rv = true;
    for (const auto& flusher : flushers) {
        if (!flusher->pause()) {
            EP_LOG_WARN(
                    "Attempted to pause flusher in state "
                    "[{}], flusher = {}",
                    flusher->stateName(),
                    flusher->getId());
            rv = false;
        }
    }

    return rv;
}

bool EPBucket::resumeFlusher() {
    bool rv = true;
    for (const auto& flusher : flushers) {
        if (!flusher->resume()) {
            EP_LOG_WARN(
                    "Attempted to resume flusher in state [{}], "
                    "flusher = {}",
                    flusher->stateName(),
                    flusher->getId());
            rv = false;
        }
    }
    return rv;
}

void EPBucket::wakeUpFlusher() {
    if (stats.diskQueueSize.load() == 0) {
        for (const auto& flusher : flushers) {
            flusher->wake();
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
    EP_LOG_INFO_RAW("Stopping bg fetchers");

    for (const auto& bgFetcher : bgFetchers) {
        bgFetcher->stop();
    }
}

void EPBucket::allVbucketsDeinitialize() {
    for (const auto& shard : vbMap.shards) {
        for (const auto vbid : shard->getVBuckets()) {
            VBucketPtr vb = shard->getBucket(vbid);
            if (vb) {
                if (vb->hasPendingBGFetchItems()) {
                    EP_LOG_WARN(
                            "Shutting down engine while there are still pending"
                            "data read for shard {} from database storage",
                            shard->getId());
                }

                // MB-53953: Ensure all RangeScans are cancelled and release
                // their snapshots
                auto& epVb = dynamic_cast<EPVBucket&>(*vb);
                epVb.cancelRangeScans();
            }
        }
    }
}

cb::engine_errc EPBucket::scheduleOrRescheduleCompaction(
        Vbid vbid,
        const CompactionConfig& config,
        CookieIface* cookie,
        std::chrono::milliseconds delay) {
    cb::engine_errc errCode = checkForDBExistence(vbid);
    if (errCode != cb::engine_errc::success) {
        return errCode;
    }

    /* Obtain the vbucket so we can get the previous purge seqno */
    VBucketPtr vb = vbMap.getBucket(vbid);
    if (!vb) {
        return cb::engine_errc::not_my_vbucket;
    }

    auto handle = compactionTasks.wlock();

    // First, let's update compaction concurrency, in case the workload
    // pattern has changed to/from READ_HEAVY.
    updateCompactionConcurrency();

    // find the earliest time the compaction task should start, to obey
    // the requested delay
    auto requestedStartTime = std::chrono::steady_clock::now() + delay;
    // Convert delay to ExecutorPool 'double' e.g. 1500ms = 1.5 secs
    std::chrono::duration<double> execDelay = delay;

    // try to emplace an empty shared_ptr
    auto [itr, emplaced] = handle->try_emplace(vbid, nullptr);
    auto& task = itr->second;
    CompactionConfig tasksConfig;

    if (!emplaced) {
        // The existing task must be poked - it needs to either reschedule if
        // it is currently running or run with the given config.
        tasksConfig = task->runCompactionWithConfig(
                config, cookie, requestedStartTime);
        // now wake the task - the config has changed and it may need to
        // work out a new wake time.
        ExecutorPool::get()->wake(task->getId());
    } else {
        // Nothing in the map for this vbid now construct the task
        task = std::make_shared<CompactTask>(*this,
                                             vbid,
                                             config,
                                             requestedStartTime,
                                             cookie,
                                             *compactionSemaphore);
        if (execDelay.count() > 0.0) {
            task->snooze(execDelay.count());
        }
        ExecutorPool::get()->schedule(task);

        tasksConfig = config;
    }

    EP_LOG_INFO(
            "Compaction of {}, task:{}, purge_before_ts:{}, "
            "purge_before_seq:{}, "
            "drop_deletes:{}, internal:{} {} with delay:{}s (awaiting "
            "completion).",
            vbid,
            task->getId(),
            tasksConfig.purge_before_ts,
            tasksConfig.purge_before_seq,
            tasksConfig.drop_deletes,
            tasksConfig.internally_requested,
            !emplaced ? "rescheduled" : "created",
            execDelay.count());

    return cb::engine_errc::would_block;
}

void EPBucket::updateCompactionConcurrency() {
    // Avoid too many concurrent compaction tasks as they
    // could impact flushing throughout (and latency) in a
    // couple of related ways:
    //
    // 1. We can only concurrently execute as many Writer
    //    tasks as we have Writer threads - if compaction
    //    tasks consume most / all of the available threads
    //    then no Flusher tasks can run.
    //
    // 2. When compacting a given vBucket, flushing the same
    //    vBucket is potentially impacted. For Couchstore we
    //    must interlock the final phase of compaction and
    //    flushing; pausing flushing while compaction "catches
    //    up" with any updates made since the last compaction
    //    iteration.
    //    (Note Magma handles its own compaction and hence
    //    isn't directly subject to this case).
    //
    // We therefore limit the number of concurrent compactors
    // in the following (somewhat non-scientific way).

    if (engine.getWorkLoadPolicy().getWorkLoadPattern() == READ_HEAVY) {
        // only allow one compaction task if the workload is read heavy
        compactionSemaphore->setCapacity(1);
        return;
    }

    const int maxConcurrentWriterTasks = std::min(
            ExecutorPool::get()->getNumWriters(), vbMap.getNumShards());
    const int maxConcurrentAuxIOTasks = ExecutorPool::get()->getNumAuxIO();
    const int compactionConcurrentTaskLimit =
            std::min(maxConcurrentWriterTasks, maxConcurrentAuxIOTasks);

    // Calculate how many compaction tasks we will permit. We always
    // allow at least one, then we limit to a fraction of the available
    // AuxIO/WriterTasks (whichever is lower) imposing an upper bound so there
    // is at least 1 AuxIO task slot available for other tasks (i.e.
    // BackfillManagerTask). We want to take the lower of AuxIO and
    // Writer threads when calculating this number as whilst we run
    // compaction tasks on the AuxIO pool we don't want to saturate disk
    // if we have few writers, and we don't want to saturate the AuxIO
    // pool if we have more writers.
    const int maxConcurrentCompactTasks = std::clamp(
            int(compactionConcurrentTaskLimit * compactionMaxConcurrency),
            1, // min of one task must be allowed to run
            maxConcurrentAuxIOTasks - 1 /* max */);

    compactionSemaphore->setCapacity(maxConcurrentCompactTasks);
}

cb::engine_errc EPBucket::scheduleCompaction(Vbid vbid,
                                             const CompactionConfig& config,
                                             CookieIface* cookie,
                                             std::chrono::milliseconds delay) {
    return scheduleOrRescheduleCompaction(vbid, config, cookie, delay);
}

cb::engine_errc EPBucket::scheduleCompaction(Vbid vbid,
                                             std::chrono::milliseconds delay) {
    CompactionConfig cfg;
    cfg.internally_requested = true;
    return scheduleOrRescheduleCompaction(vbid, cfg, nullptr, delay);
}

cb::engine_errc EPBucket::cancelCompaction(Vbid vbid) {
    auto handle = compactionTasks.wlock();
    for (const auto& task : *handle) {
        task.second->cancel();
    }
    return cb::engine_errc::success;
}

void EPBucket::flushOneDelOrSet(TransactionContext& txnCtx,
                                const queued_item& qi,
                                VBucketPtr& vb) {
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

    auto* rwUnderlying = getRWUnderlying(qi->getVBucketId());
    if (!deleted) {
        // TODO: Need to separate disk_insert from disk_update because
        // bySeqno doesn't give us that information.
        HdrMicroSecBlockTimer timer(
                bySeqno == -1 ? &stats.diskInsertHisto : &stats.diskUpdateHisto,
                bySeqno == -1 ? "disk_insert" : "disk_update",
                stats.timingLog.get());
        if (qi->isSystemEvent()) {
            rwUnderlying->setSystemEvent(txnCtx, qi);
        } else {
            rwUnderlying->set(txnCtx, qi);
        }
    } else {
        if (qi->deletionSource() == DeleteSource::TTL) {
            engine.getServerApi()->document->document_expired(engine,
                                                              qi->getNBytes());
        }
        HdrMicroSecBlockTimer timer(
                &stats.diskDelHisto, "disk_delete", stats.timingLog.get());
        if (qi->isSystemEvent()) {
            rwUnderlying->delSystemEvent(txnCtx, qi);
        } else {
            rwUnderlying->del(txnCtx, qi);
        }
    }
}

void EPBucket::dropKey(VBucket& vb,
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

    auto docKey = diskKey.getDocKey();
    if (docKey.isInSystemCollection()) {
        throw std::logic_error("EPBucket::dropKey called for a system key");
    }

    if (diskKey.isPrepared() && bySeqno > highCompletedSeqno) {
        // ... drop it from the DurabilityMonitor
        vb.dropPendingKey(docKey, bySeqno);
    }

    // ... drop it from the VB (hashtable)
    vb.dropKey(docKey, bySeqno);
}

std::shared_ptr<CompactionContext> EPBucket::makeCompactionContext(
        Vbid vbid, CompactionConfig& config, uint64_t purgeSeqno) {
    auto& configuration = getEPEngine().getConfiguration();

    if (config.purge_before_ts == 0) {
        config.purge_before_ts =
                ep_real_time() - configuration.getPersistentMetadataPurgeAge();
    }

    std::optional<time_t> expireFrom;
    if (configuration.isCompactionExpireFromStart()) {
        expireFrom = ep_real_time();
    }

    auto vb = getVBucket(vbid);
    if (!vb) {
        return nullptr;
    }

    auto ctx = std::make_shared<CompactionContext>(
            std::move(vb), config, purgeSeqno, expireFrom);

    BloomFilterCBPtr filter(new BloomFilterCallback(*this));
    ctx->bloomFilterCallback = filter;

    ExpiredItemsCBPtr expiry(new ExpiredItemsCallback(*this));
    ctx->expiryCallback = expiry;

    // take a raw ref to the context as if the function is being called we know
    // it's not out of scope, so there's no need to add a ref to it.
    ctx->droppedKeyCb = [this, &ctxRef = *ctx](const DiskDocKey& diskKey,
                                               int64_t bySeqno,
                                               bool isAbort,
                                               int64_t highCompletedSeqno) {
        dropKey(*ctxRef.getVBucket(),
                diskKey,
                bySeqno,
                isAbort,
                highCompletedSeqno);
    };

    ctx->completionCallback = [this](CompactionContext& ctx) {
        compactionCompletionCallback(ctx);
    };

    // take a raw ref to the context as if the function is being called we know
    // it's not out of scope, so there's no need to add a ref to it.
    ctx->maybeUpdateVBucketPurgeSeqno =
            [this, &ctxRef = *ctx](uint64_t seqno) -> void {
        ctxRef.getVBucket()->maybeSetPurgeSeqno(seqno);
        postPurgeSeqnoImplicitCompactionHook();
    };

    auto& epStats = getEPEngine().getEpStats();
    ctx->isShuttingDown = [&epStats, &ctxRef = *ctx, this]() -> bool {
        // stop compaction if the bucket is shutting down, the vbucket is
        // awaiting deferred deletion.
        return epStats.isShutdown ||
               ctxRef.getVBucket()->isDeletionDeferred() ||
               cancelEWBCompactionTasks;
    };
    return ctx;
}

void EPBucket::compactionCompletionCallback(CompactionContext& ctx) {
    auto vb = ctx.getVBucket();

    // Grab a pre-compaction snapshot of the stats
    auto prePurgeSeqno = vb->getPurgeSeqno();
    auto preNumTotalItems = vb->getNumTotalItems();

    auto preCollectionSizes = ctx.stats.collectionSizeUpdates;
    bool preDropInProgess = vb->getManifest().isDropInProgress();
    for (auto& [cid, newSize] : preCollectionSizes) {
        auto handle = vb->getManifest().lock(cid);

        if (handle.valid()) {
            newSize = handle.getDiskSize();
        }
    }

    try {
        postCompactionCompletionStatsUpdateHook();

        vb->maybeSetPurgeSeqno(ctx.getRollbackPurgeSeqno());
        vb->decrNumTotalItems(ctx.stats.collectionsItemsPurged);

        updateCollectionStatePostCompaction(
                *vb,
                ctx.stats.collectionSizeUpdates,
                ctx.eraserContext->doesOnDiskDroppedDataExist());

    } catch (std::exception&) {
        // Re-apply our pre-compaction stats snapshot
        vb->maybeSetPurgeSeqno(prePurgeSeqno);
        vb->setNumTotalItems(preNumTotalItems);

        updateCollectionStatePostCompaction(
                *vb, preCollectionSizes, preDropInProgess);

        // And re-throw to "undo" the on disk compaction
        throw;
    }
}

void EPBucket::updateCollectionStatePostCompaction(
        VBucket& vb,
        CompactionStats::CollectionSizeUpdates& collectionSizeUpdates,
        bool onDiskDroppedCollectionDataExists) {
    for (const auto& [cid, newSize] : collectionSizeUpdates) {
        auto handle = vb.getManifest().lock(cid);

        if (handle.valid()) {
            handle.setDiskSize(newSize);
        }
    }

    // Set the dropInProgress flag to true if ondisk dropped data exists
    vb.getManifest().setDropInProgress(onDiskDroppedCollectionDataExists);
}

bool EPBucket::compactInternal(LockedVBucketPtr& vb, CompactionConfig& config) {
    auto ctx = makeCompactionContext(vb->getId(), config, vb->getPurgeSeqno());
    auto* shard = vbMap.getShardByVbId(vb->getId());
    auto* store = shard->getRWUnderlying();
    CompactDBStatus result;
    try {
        result = store->compactDB(vb.getLock(), ctx);
    } catch (const std::exception& e) {
        EP_LOG_ERR("EPBucket::compactInternal(): compactDB() threw:'{}'",
                   e.what());
        stats.compactionFailed++;
        return false;
    }

    switch (result) {
    case CompactDBStatus::Failed:
        EP_LOG_ERR("EPBucket::compactInternal: compaction failed for {}",
                   vb->getId());
        stats.compactionFailed++;
        break;
    case CompactDBStatus::Aborted:
        EP_LOG_DEBUG("EPBucket::compactInternal: compaction aborted for {}",
                    vb->getId());
        stats.compactionAborted++;
        break;
    case CompactDBStatus::Success:
        break;
    }

    if (getEPEngine().getConfiguration().isBfilterEnabled() &&
        result == CompactDBStatus::Success) {
        vb->swapFilter();
    } else {
        vb->clearFilter();
    }

    EP_LOG_INFO(
            "Compaction of {} done ({}). "
            "purged tombstones:{}, prepares:{}, prepareBytes:{} "
            "collection_items_erased:alive:{},deleted:{}, "
            "collections_erased:{}, "
            "size/items/tombstones/purge_seqno pre{{{}, {}, {}, {}}}, "
            "post{{{}, {}, {}, {}}}",
            vb->getId(),
            result,
            ctx->stats.tombstonesPurged,
            ctx->stats.preparesPurged,
            ctx->stats.prepareBytesPurged,
            ctx->stats.collectionsItemsPurged,
            ctx->stats.collectionsDeletedItemsPurged,
            ctx->stats.collectionsPurged,
            ctx->stats.pre.size,
            ctx->stats.pre.items,
            ctx->stats.pre.deletedItems,
            ctx->stats.pre.purgeSeqno,
            ctx->stats.post.size,
            ctx->stats.post.items,
            ctx->stats.post.deletedItems,
            ctx->stats.post.purgeSeqno);
    return result == CompactDBStatus::Success;
}

// Running on WriterTask - CompactTask
bool EPBucket::doCompact(Vbid vbid,
                         CompactionConfig& config,
                         std::vector<CookieIface*>& cookies) {
    cb::engine_errc status = cb::engine_errc::success;

    auto vb = getLockedVBucket(vbid, std::try_to_lock);
    if (!vb.owns_lock()) {
        // VB currently locked; try again later.
        return true;
    }

    /**
     * MB-30015: Check to see if tombstones that have invalid
     * data needs to be retained. The goal is to try and retain
     * the erroneous tombstones especially in customer environments
     * for further analysis
     */
    config.retain_erroneous_tombstones = isRetainErroneousTombstones();

    bool reschedule = false;
    if (vb) {
        if (!compactInternal(vb, config)) {
            status = cb::engine_errc::failed;

            // Only if an internal request was made should we reschedule. If
            // compaction came externally, it is up to the client to retry
            reschedule = config.internally_requested;
        }
    } else {
        status = cb::engine_errc::not_my_vbucket;
    }

    if (status != cb::engine_errc::success && !cookies.empty()) {
        // The memcached core won't call back into the engine if the error
        // code returned in notifyIOComplete is != success so we need to
        // do all of the clean-up here.
        for (const auto& cookie : cookies) {
            engine.clearEngineSpecific(*cookie);
        }
    }

    for (const auto& cookie : cookies) {
        engine.notifyIOComplete(cookie, status);
    }
    // All cookies notified so clear the container
    cookies.clear();

    --stats.pendingCompactions;
    return reschedule;
}

bool EPBucket::updateCompactionTasks(Vbid vbid) {
    auto handle = compactionTasks.wlock();

    // remove the calling task if it does not need to run again.
    if (auto itr = handle->find(vbid); itr != handle->end()) {
        const auto& task = (*itr).second;
        if (task->isRescheduleRequired()) {
            // Nope can't erase! Must re-schedule this task.
            return true;
        } else {
            // Done, can now erase from the compactionTasks map
            handle->erase(itr);
        }
    } else {
        throw std::logic_error(
                "EPBucket::updateCompactionTasks no task for vbid:" +
                vbid.to_string());
    }

    return false;
}

DBFileInfo EPBucket::getAggregatedFileInfo() {
    const auto numShards = vbMap.getNumShards();
    DBFileInfo totalInfo;

    for (uint16_t shardId = 0; shardId < numShards; shardId++) {
        totalInfo += getRWUnderlyingByShard(shardId)->getAggrDbFileInfo();
    }
    return totalInfo;
}

uint64_t EPBucket::getTotalDiskSize() {
    using namespace cb::stats;
    return getAggregatedFileInfo().fileSize;
}

cb::engine_errc EPBucket::getFileStats(const BucketStatCollector& collector) {
    auto totalInfo = getAggregatedFileInfo();

    using namespace cb::stats;
    collector.addStat(Key::ep_db_data_size, totalInfo.getEstimatedLiveData());
    collector.addStat(Key::ep_db_file_size, totalInfo.fileSize);
    collector.addStat(Key::ep_db_prepare_size, totalInfo.prepareBytes);

    return cb::engine_errc::success;
}

cb::engine_errc EPBucket::getPerVBucketDiskStats(CookieIface& cookie,
                                                 const AddStatFn& add_stat) {
    class DiskStatVisitor : public VBucketVisitor {
    public:
        DiskStatVisitor(CookieIface& c, AddStatFn a)
            : cookie(c), add_stat(std::move(a)) {
        }

        void visitBucket(VBucket& vb) override {
            std::array<char, 32> buf;
            Vbid vbid = vb.getId();
            try {
                auto dbInfo =
                        vb.getShard()->getRWUnderlying()->getDbFileInfo(vbid);

                checked_snprintf(
                        buf.data(), buf.size(), "vb_%d:data_size", vbid.get());
                add_casted_stat(buf.data(),
                                dbInfo.getEstimatedLiveData(),
                                add_stat,
                                cookie);
                checked_snprintf(
                        buf.data(), buf.size(), "vb_%d:file_size", vbid.get());
                add_casted_stat(buf.data(), dbInfo.fileSize, add_stat, cookie);
                checked_snprintf(buf.data(),
                                 buf.size(),
                                 "vb_%d:prepare_size",
                                 vbid.get());
                add_casted_stat(
                        buf.data(), dbInfo.prepareBytes, add_stat, cookie);
            } catch (std::exception& error) {
                EP_LOG_WARN(
                        "DiskStatVisitor::visitBucket: Failed to build stat: "
                        "{}",
                        error.what());
            }
        }

    private:
        CookieIface& cookie;
        AddStatFn add_stat;
    };

    DiskStatVisitor dsv(cookie, add_stat);
    visit(dsv);
    return cb::engine_errc::success;
}

size_t EPBucket::getPageableMemCurrent() const {
    // EP Buckets can (theoretically) page out all memory, active(+pending) or
    // replica.
    return stats.getEstimatedTotalMemoryUsed();
}

size_t EPBucket::getPageableMemHighWatermark() const {
    return stats.mem_high_wat;
}

size_t EPBucket::getPageableMemLowWatermark() const {
    return stats.mem_low_wat;
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
        uint64_t maxVisibleSeqno,
        uint64_t maxPrepareSeqno) {
    // Not using make_shared or allocate_shared
    // 1. make_shared doesn't accept a Deleter
    // 2. allocate_shared has inconsistencies between platforms in calling
    //    alloc.destroy (libc++ doesn't call it)
    return {new EPVBucket(id,
                          state,
                          stats,
                          engine.getCheckpointConfig(),
                          shard,
                          lastSeqno,
                          lastSnapStart,
                          lastSnapEnd,
                          std::move(table),
                          std::make_shared<NotifyFlusherCB>(shard),
                          std::move(newSeqnoCb),
                          makeSyncWriteResolvedCB(),
                          makeSyncWriteCompleteCB(),
                          syncWriteTimeoutFactory,
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
                          maxVisibleSeqno,
                          maxPrepareSeqno),
            VBucket::DeferredDeleter(engine)};
}

cb::engine_errc EPBucket::statsVKey(const DocKey& key,
                                    Vbid vbucket,
                                    CookieIface* cookie) {
    VBucketPtr vb = getVBucket(vbucket);
    if (!vb) {
        return cb::engine_errc::not_my_vbucket;
    }

    return vb->statsVKey(key, cookie, engine);
}

void EPBucket::completeStatsVKey(CookieIface* cookie,
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

    if (gcb.getStatus() == cb::engine_errc::success) {
        engine.addLookupResult(*cookie, std::move(gcb.item));
    } else {
        engine.addLookupResult(*cookie, nullptr);
    }

    --stats.numRemainingBgJobs;
    engine.notifyIOComplete(cookie, cb::engine_errc::success);
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
                                        ValueFilter::VALUES_DECOMPRESSED);

        // This is the item in the state it was before the rollback seqno
        // (i.e. the desired state). null if there was no previous
        // Item.
        UniqueItemPtr preRbSeqnoItem(std::move(preRbSeqnoGetValue.item));

        if (preRbSeqnoGetValue.getStatus() == cb::engine_errc::success) {
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
                                yield();
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
            }
        } else if (preRbSeqnoGetValue.getStatus() ==
                   cb::engine_errc::no_such_key) {
            EP_LOG_DEBUG_RAW(
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
        vb.removeItemFromMemory(item);
        // If the doc was or was not in memory, still set status as success, the
        // rollback can continue. !success here will cancel the scan
        setStatus(cb::engine_errc::success);
    }

private:
    EventuallyPersistentEngine& engine;

    /// The seqno to which we are rolling back
    uint64_t rollbackSeqno;
};

std::unique_ptr<RollbackCtx> EPBucket::prepareToRollback(Vbid vbid) {
    auto* rwUnderlying = vbMap.getShardByVbId(vbid)->getRWUnderlying();
    return rwUnderlying->prepareToRollback(vbid);
}

RollbackResult EPBucket::doRollback(Vbid vbid, uint64_t rollbackSeqno) {
    auto* rwUnderlying = vbMap.getShardByVbId(vbid)->getRWUnderlying();
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

            if (gcb.getStatus() == cb::engine_errc::success) {
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
        VBucket& vb) {
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
                yield(); // Trigger yield and return from KVStore::scan
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
                if (val.item->getKey().isInDefaultCollection()) {
                    highestDefaultCollectionVisible =
                            std::max(highestDefaultCollectionVisible,
                                     uint64_t(val.item->getBySeqno()));
                }
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

        uint64_t highestDefaultCollectionVisible = 0;

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
    auto vbState = kvStore->getCachedVBucketState(epVb.getId());
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
        epVb.loadOutstandingPrepares(*vbState, std::move(prepares));
        // No prepares loaded
        return {0, 0, 0, true};
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
    if (isDiskCheckpointType(vbState->checkpointType) &&
        static_cast<uint64_t>(vbState->highSeqno) != vbState->lastSnapEnd) {
        endSeqno = vbState->highSeqno;

        EP_LOG_INFO(
                "EPBucket::loadPreparedSyncWrites: {} current snapshot is "
                "disk type and incomplete so loading all prepares from: "
                "{} to {}",
                vb.getId(),
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
        return {0, 0, false};
    }

    auto scanResult = kvStore->scan(*scanCtx);

    switch (scanResult) {
    case ScanStatus::Success:
        break;
    case ScanStatus::Yield:
        // If we abort our scan early due to reaching the HPS (by setting
        // storageCB.getStatus) then the scan result will be 'Yield' but we
        // will have scanned correctly.
        break;
    case ScanStatus::Failed: {
        EP_LOG_CRITICAL(
                "EPBucket::loadPreparedSyncWrites: scan() failed for {}",
                epVb.getId());
        return {0, 0, false};
    }
    case ScanStatus::Cancelled: {
        // Cancelled is not expected as the callbacks never set a runtime error
        Expects(ScanStatus::Cancelled != scanResult);
    }
    }

    auto& storageCB = static_cast<LoadSyncWrites&>(scanCtx->getValueCallback());
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
    epVb.loadOutstandingPrepares(*vbState, std::move(prepares));

    return {storageCB.itemsVisited,
            numPrepares,
            storageCB.highestDefaultCollectionVisible,
            true};
}

ValueFilter EPBucket::getValueFilterForCompressionMode(CookieIface* cookie) {
    auto compressionMode = engine.getCompressionMode();
    auto filter = ValueFilter::VALUES_COMPRESSED;
    if (compressionMode == BucketCompressionMode::Off) {
        filter = ValueFilter::VALUES_DECOMPRESSED;
    }
    if (cookie &&
        !cookie->isDatatypeSupported(PROTOCOL_BINARY_DATATYPE_SNAPPY)) {
        filter = ValueFilter::VALUES_DECOMPRESSED;
    }
    return filter;
}

void EPBucket::notifyNewSeqno(const Vbid vbid, const VBNotifyCtx& notifyCtx) {
    if (notifyCtx.notifyFlusher) {
        notifyFlusher(vbid);
    }
    if (notifyCtx.notifyReplication) {
        notifyReplication(vbid, notifyCtx.syncWrite);
    }
}

Warmup* EPBucket::getWarmup() const {
    return warmupTask.get();
}

bool EPBucket::isWarmupLoadingData() {
    return warmupTask && !warmupTask->isFinishedLoading();
}

bool EPBucket::isWarmupComplete() {
    return warmupTask && warmupTask->isComplete();
}

bool EPBucket::isWarmupOOMFailure() {
    return warmupTask && warmupTask->hasOOMFailure();
}

bool EPBucket::hasWarmupSetVbucketStateFailed() const {
    return warmupTask && warmupTask->hasSetVbucketStateFailure();
}

bool EPBucket::maybeWaitForVBucketWarmup(CookieIface* cookie) {
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
    if (!engine.getConfiguration().getAlogPath().empty()) {
        if (engine.getConfiguration().isAccessScannerEnabled()) {
            accessScanner.wlock()->enabled = true;
            EP_LOG_INFO_RAW("Access Scanner task enabled");
            auto smin = engine.getConfiguration().getAlogSleepTime();
            setAccessScannerSleeptime(smin, true);
        } else {
            accessScanner.wlock()->enabled = false;
            EP_LOG_INFO_RAW("Access Scanner task disabled");
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
    if (isWarmupLoadingData()) {
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
        CookieIface* cookie,
        std::unique_ptr<Collections::Manifest>& newManifest) {
    getEPEngine().storeEngineSpecific(*cookie, newManifest.get());

    ExTask task = std::make_shared<Collections::PersistManifestTask>(
            *this, std::move(newManifest), cookie);
    ExecutorPool::get()->schedule(task);
    return true; // we took the manifest
}

std::ostream& operator<<(std::ostream& os, const EPBucket::FlushResult& res) {
    os << std::boolalpha << "moreAvailable:{"
       << (res.moreAvailable == EPBucket::MoreAvailable::Yes) << "} "
       << "numFlushed:{" << res.numFlushed << "}";
    return os;
}

BgFetcher& EPBucket::getBgFetcher(Vbid vbid) {
    // For now we just use modulo, same as we do/would for shards to pick out
    // the associated BgFetcher
    auto id = vbid.get() % bgFetchers.size();
    return *bgFetchers.at(id);
}

Flusher* EPBucket::getFlusher(Vbid vbid) {
    // For now we just use modulo, same as we do/would for shards to pick out
    // the associated BgFetcher
    auto id = vbid.get() % flushers.size();
    return flushers.at(id).get();
}

Flusher* EPBucket::getOneFlusher() {
    Expects(flushers.size() > 0);
    return flushers.front().get();
}

void EPBucket::releaseBlockedCookies() {
    KVBucket::releaseBlockedCookies();

    // Stop warmup (if not yet completed) which will unblock any cookies which
    // were held pending if they were received before populateVBucketMap phase.
    stopWarmup();

    // Abort any running compactions, there's no point running them for any
    // external clients because we're disconnecting them.
    cancelEWBCompactionTasks = true;

    // It's not enough to abort any running compactions though. We could have
    // some waiting in the queue behind other tasks for other buckets which are
    // not getting cancelled. As such, we should notify the cookies to
    // disconnect them and let the tasks get cleaned up later.
    auto compactionsHandle = compactionTasks.wlock();
    for (auto& [vbid, task] : *compactionsHandle) {
        auto cookies = task->takeCookies();
        for (const auto& cookie : cookies) {
            // The status doesn't really matter here, it gets returned as
            // success if we are completing a blocked request but it feels wrong
            // to return success here.
            engine.notifyIOComplete(cookie, cb::engine_errc::failed);
        }
    }
}

std::shared_ptr<RangeScan> EPBucket::takeNextRangeScan(size_t taskId) {
    return rangeScans.takeNextScan(taskId);
}

std::pair<cb::engine_errc, cb::rangescan::Id> EPBucket::createRangeScan(
        Vbid vbid,
        CollectionID cid,
        cb::rangescan::KeyView start,
        cb::rangescan::KeyView end,
        std::unique_ptr<RangeScanDataHandlerIFace> handler,
        CookieIface& cookie,
        cb::rangescan::KeyOnly keyOnly,
        std::optional<cb::rangescan::SnapshotRequirements> snapshotReqs,
        std::optional<cb::rangescan::SamplingConfiguration> samplingConfig) {
    auto vb = getVBucket(vbid);
    if (!vb) {
        ++stats.numNotMyVBuckets;
        return {cb::engine_errc::not_my_vbucket, {}};
    }

    auto handle = vb->getManifest().lock(cid);
    cb::engine_errc status = cb::engine_errc::success;
    if (!handle.valid()) {
        engine.setUnknownCollectionErrorContext(cookie,
                                                handle.getManifestUid());

        status = cb::engine_errc::unknown_collection;
    } else if (snapshotReqs) {
        if (vb->failovers->getLatestUUID() != snapshotReqs->vbUuid) {
            status = cb::engine_errc::vbuuid_not_equal;
        } else if (vb->getPersistenceSeqno() < snapshotReqs->seqno &&
                   !snapshotReqs->timeout) {
            status = cb::engine_errc::temporary_failure;
        }
    }

    if (status != cb::engine_errc::success) {
        // This maybe the continuation of a create - the create task may of
        // succeeded, yet the vb/collection has now gone - require a clean-up
        auto& epVb = dynamic_cast<EPVBucket&>(*vb);
        auto checkStatus = epVb.checkAndCancelRangeScanCreate(cookie);

        // A task (to cancel) must of been scheduled (would_block)
        // This is the first part of the request (success)
        if (checkStatus != cb::engine_errc::would_block &&
            checkStatus != cb::engine_errc::success) {
            EP_LOG_WARN(
                    "{} createRangeScan failed to cancel for {} status:{} "
                    "checkStatus:{}",
                    vb,
                    cid,
                    status,
                    checkStatus);
        }
        return {status, {}};
    }

    return vb->createRangeScan(cid,
                               start,
                               end,
                               std::move(handler),
                               cookie,
                               keyOnly,
                               snapshotReqs,
                               samplingConfig);
}

cb::engine_errc EPBucket::continueRangeScan(Vbid vbid,
                                            cb::rangescan::Id uuid,
                                            CookieIface& cookie,
                                            size_t itemLimit,
                                            std::chrono::milliseconds timeLimit,
                                            size_t byteLimit) {
    auto vb = getVBucket(vbid);
    if (!vb) {
        ++stats.numNotMyVBuckets;
        return cb::engine_errc::not_my_vbucket;
    }

    return vb->continueRangeScan(uuid, cookie, itemLimit, timeLimit, byteLimit);
}

cb::engine_errc EPBucket::cancelRangeScan(Vbid vbid,
                                          cb::rangescan::Id uuid,
                                          CookieIface& cookie) {
    auto vb = getVBucket(vbid);
    if (!vb) {
        ++stats.numNotMyVBuckets;
        return cb::engine_errc::not_my_vbucket;
    }
    return vb->cancelRangeScan(
            uuid, &cookie, true /* schedule for background cancel */);
}

cb::engine_errc EPBucket::prepareForPause(
        folly::CancellationToken cancellationToken) {
    // 1. Wait for all outstanding disk writing operations to complete.
    // a) Flusher, Rollback, DeleteVB - These all require that the
    //    appropriate vb_mutexes element has been acquired, so we simply
    //    acquire _all_ of them here, which means any of the above in-flight
    //    operations will have to complete before we continue - and that no
    //    more can begin.
    //    Once all preparations have been completed; we set EPBucket::paused
    //    to true and unlock the mutexes - any future attempts to lock them
    //    will be blocked until EPBucket::paused is cleared
    //    (via prepareForResume()).
    EP_LOG_DEBUG_RAW(
            "EPBucket::prepareForPause: waiting for in-flight Flusher, "
            "Rollback, DeleteVB tasks to complete");
    std::vector<std::unique_lock<std::mutex>> vb_locks;
    for (auto& mutex : vb_mutexes) {
        vb_locks.emplace_back(mutex);
    }

    // b) Compaction - This only requires that the appropriate vb_mutexes is
    //    acquired for critical sections (assuming that the KVStore supports
    //    concurrent compaction, which Couchstore & Magma do). As such, we need
    //    to do more - we reduce the capacity of the compaction semaphore to
    //    1, then acquire this last token. This prevents any new compaction
    //    tasks from running, and if are compaction tasks already then
    //    we block acquiring the singular token until they have finished.
    EP_LOG_DEBUG_RAW(
            "EPBucket::prepareForPause: waiting for all Compaction tasks to "
            "complete");
    compactionSemaphore->setCapacity(1);
    struct BlockingWaiter : public cb::Waiter {
        BlockingWaiter(cb::AwaitableSemaphore& sem) : semaphore{sem} {
        }
        cb::AwaitableSemaphore& semaphore;
        folly::Baton<> baton;
        void signal() override {
            // Note: signal() only indicates that we _might_ now be able to
            // acquire a token, it doesn't mean the token has been returned.
            if (semaphore.acquire_or_wait(weak_from_this())) {
                baton.post();
            }
        }
    };
    auto waiter = std::make_shared<BlockingWaiter>(*compactionSemaphore);
    // "Signal" the waiter - ie. check if we can acquire the semaphore now.
    // (This is done to avoid repeating the same initial acquirw_or_wait()
    //  logic here).
    waiter->signal();
    // And wait until the waiter successfully acquires a token.
    waiter->baton.wait();

    // 2. Tell all the KVStores to pause. This ensures any background IO
    //    operations which ep-engine is unaware of are also completed (and no
    //    new ones started).
    bool allSuccess = true;
    vbMap.forEachShard([&allSuccess](KVShard& shard) {
        EP_LOG_DEBUG("EPBucket::prepareForPause: pausing KVShard:{}", shard.getId());

        bool success = shard.getRWUnderlying()->pause();
        if (!success) {
            EP_LOG_WARN("EPBucket::prepareForPause: shard:{} failed",
                        shard.getId());
        }
        allSuccess &= success;
    });

    if (allSuccess) {
        // Successfully prepared for pausing; set paused flag to true before
        // we unlock all the vb_mutexes; that will inhibit anyone from acquiring
        // the mutexes again until paused is set to false.
        paused.store(true);
        return cb::engine_errc::success;
    }
    return cb::engine_errc::failed;
}

cb::engine_errc EPBucket::prepareForResume() {
    // This is the reverse of prepareForResume() - see that for more details.

    // 1. Tell all KVStores to resume. This allows them schedule background
    //    write operations on their own accord, and also perform write
    //    operations issued by ep-engine.
    vbMap.forEachShard([](KVShard& shard) {
        EP_LOG_DEBUG("EPBucket::prepareForResume: resuming KVShard:{}",
                     shard.getId());
        shard.getRWUnderlying()->resume();
    });

    // 2. Resume ep-engine operations.
    // a) Clear EPBucket::paused so disk writing operations can
    //    resume.
    EP_LOG_DEBUG_RAW(
            "EPBucket::prepareForResume: unblocking all Flusher, "
            "Rollback, DeleteVB tasks.");
    paused.store(false);

    // b) Reset compaction concurrency
    EP_LOG_DEBUG_RAW(
            "EPBucket::prepareForResume: resuming all Compaction tasks");
    compactionSemaphore->release();
    updateCompactionConcurrency();

    return cb::engine_errc::success;
}
