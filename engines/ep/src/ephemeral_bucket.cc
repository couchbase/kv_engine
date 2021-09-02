/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2016-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "ephemeral_bucket.h"

#include "bucket_logger.h"
#include "checkpoint_manager.h"
#include "collections/vbucket_manifest.h"
#include "ep_engine.h"
#include "ep_types.h"
#include "ephemeral_tombstone_purger.h"
#include "ephemeral_vb.h"
#include "ephemeral_vb_count_visitor.h"
#include "failover-table.h"
#include "replicationthrottle.h"
#include "rollback_result.h"
#include <executor/executorpool.h>

#include <statistics/collector.h>
#include <statistics/labelled_collector.h>

#include <algorithm>

/**
 * A configuration value changed listener that responds to Ephemeral bucket
 * parameter changes.
 */
class EphemeralValueChangedListener : public ValueChangedListener {
public:
    explicit EphemeralValueChangedListener(EphemeralBucket& bucket)
        : bucket(bucket) {
    }

    void stringValueChanged(const std::string& key,
                            const char* value) override {
        if (key == "ephemeral_full_policy") {
            if (std::string_view(value) == "auto_delete") {
                bucket.enableItemPager();
            } else if (std::string_view(value) == "fail_new_data") {
                bucket.disableItemPager();
            } else {
                EP_LOG_WARN(
                        "EphemeralValueChangedListener: Invalid value '{}' for "
                        "'ephemeral_full_policy - ignoring.",
                        value);
            }
        } else {
            EP_LOG_WARN(
                    "EphemeralValueChangedListener: Failed to change value for "
                    "unknown key '{}'",
                    key);
        }
    }

    void ssizeValueChanged(const std::string& key, ssize_t value) override {
        if (key == "ephemeral_metadata_purge_age") {
            if (value == -1) {
                bucket.disableTombstonePurgerTask();
            }
            // Any non-negative value will be picked up by the Task the
            // next time it runs.
        } else {
            EP_LOG_WARN(
                    "EphemeralValueChangedListener: Failed to change value for "
                    "unknown key '{}'",
                    key);
        }
    }

    void sizeValueChanged(const std::string& key, size_t value) override {
        if (key == "ephemeral_metadata_purge_interval") {
            // Cancel and re-schedule the task to pick up the new interval.
            bucket.enableTombstonePurgerTask();
        } else {
            EP_LOG_WARN(
                    "EphemeralValueChangedListener: Failed to change value for "
                    "unknown key '{}'",
                    key);
        }
    }

private:
    EphemeralBucket& bucket;
};

EphemeralBucket::EphemeralBucket(EventuallyPersistentEngine& theEngine)
    : KVBucket(theEngine),
      notifyHpReqTask(std::make_shared<NotifyHighPriorityReqTask>(theEngine)) {
    /* We always have EvictionPolicy::Value eviction policy because a key not
       present in HashTable implies key not present at all.
       Note: This should not be confused with the eviction algorithm
             that we are going to use like NRU, FIFO etc. */
    eviction_policy = EvictionPolicy::Value;

    // Create tombstone purger task; it will later be scheduled as necessary
    // in initialize().
    tombstonePurgerTask =
            std::make_shared<EphTombstoneHTCleaner>(&engine, *this);

    replicationThrottle = std::make_unique<ReplicationThrottleEphe>(
            engine.getConfiguration(), stats);
}

EphemeralBucket::~EphemeralBucket() {
    ExecutorPool::get()->cancel(notifyHpReqTask->getId());
}

bool EphemeralBucket::initialize() {
    KVBucket::initialize();
    auto& config = engine.getConfiguration();

    // Item pager - only scheduled if "auto_delete" is specified as the bucket
    // full policy, but always add a value changed listener so we can handle
    // dynamic config changes (and later schedule it).
    if (config.getEphemeralFullPolicy() == "auto_delete") {
        enableItemPager();
    }
    engine.getConfiguration().addValueChangedListener(
            "ephemeral_full_policy",
            std::make_unique<EphemeralValueChangedListener>(*this));

    // Tombstone purger - scheduled periodically as long as we have a
    // non-zero interval. Can be dynamically adjusted, so add config listeners.
    auto interval = config.getEphemeralMetadataPurgeInterval();
    if (interval > 0) {
        enableTombstonePurgerTask();
    }
    config.addValueChangedListener(
            "ephemeral_metadata_purge_age",
            std::make_unique<EphemeralValueChangedListener>(*this));
    config.addValueChangedListener(
            "ephemeral_metadata_purge_interval",
            std::make_unique<EphemeralValueChangedListener>(*this));

    // High priority vbucket request notification task
    ExecutorPool::get()->schedule(notifyHpReqTask);

    return true;
}

size_t EphemeralBucket::getPageableMemCurrent() const {
    // Ephemeral buckets differ from persistent in terms of how memory can
    // be freed - only active items can (potentially) be directly deleted
    // (auto-delete or items which have expired) - given that the replica
    // must be an exact copy of the active.
    // As such, 'pageable' memory is non-replica memmory.

    // We don't directly track "active_mem_used", but we can roughtly estimate
    // it from mem_used - replica_ht_mem - replica_checkpoint_mem
    const auto estimatedActiveMemory =
            int64_t(stats.getEstimatedTotalMemoryUsed()) -
            stats.replicaHTMemory - stats.replicaCheckpointOverhead;
    return std::max(estimatedActiveMemory, int64_t(0));
}

size_t EphemeralBucket::getPageableMemHighWatermark() const {
    // Ephemeral buckets can only page out non-replica memory (see comments
    // in getPageableMemCurrent). As such, set pagable high watermark to
    // a fraction of the overall high watermark based on what faction of
    // vBuckets are active. Memory used by any dead vbs should be reclaimed
    // soon, so ignore them here; they don't need to be allocated a portion
    // of the quota.
    const double activeVBCount = vbMap.getVBStateCount(vbucket_state_active);
    const double pendingVBCount = vbMap.getVBStateCount(vbucket_state_pending);
    const double replicaVBCount = vbMap.getVBStateCount(vbucket_state_replica);
    const double totalVBCount = activeVBCount + pendingVBCount + replicaVBCount;

    if (totalVBCount <= 0) {
        // not an expected situation, bail out and return the full high
        // watermark
        return stats.mem_high_wat.load();
    }

    const double activePendingHighWat =
            (stats.mem_high_wat.load() / totalVBCount) *
            (activeVBCount + pendingVBCount);

    return activePendingHighWat;
}

size_t EphemeralBucket::getPageableMemLowWatermark() const {
    // Ephemeral buckets can only page out non-replica memory (see comments
    // in getPageableMemCurrent). As such, set pagable low watermark to
    // a fraction of the overall low watermark based on what faction of
    // vBuckets are active. Memory used by any dead vbs should be reclaimed
    // soon, so ignore them here; they don't need to be allocated a portion
    // of the quota.
    const double activeVBCount = vbMap.getVBStateCount(vbucket_state_active);
    const double pendingVBCount = vbMap.getVBStateCount(vbucket_state_pending);
    const double replicaVBCount = vbMap.getVBStateCount(vbucket_state_replica);
    const double totalVBCount = activeVBCount + pendingVBCount + replicaVBCount;

    if (totalVBCount <= 0) {
        // not an expected situation, bail out and return the full low
        // watermark
        return stats.mem_low_wat.load();
    }

    const double activeLowWat = (stats.mem_low_wat.load() / totalVBCount) *
                                (activeVBCount + pendingVBCount);

    return activeLowWat;
}

void EphemeralBucket::attemptToFreeMemory() {
    // Call down to the base class; do to whatever it can to free memory.
    KVBucket::attemptToFreeMemory();

    // No-eviction Ephemeral buckets don't schedule the item pager, which
    // normally handles deleting expired items (while looking for items to
    // evict). Instead manually trigger the expiryPager now, to delete any
    // expired items.
    if (engine.getConfiguration().getEphemeralFullPolicy() == "fail_new_data") {
        wakeUpExpiryPager();
    }

    // Additionally, wake up the tombstone purger to scan for and remove any
    // tombstones in the HashTable / sequence list.
    if (tombstonePurgerTask->getState() == TASK_SNOOZED) {
        ExecutorPool::get()->wake(tombstonePurgerTask->getId());
    }
}

cb::engine_errc EphemeralBucket::scheduleCompaction(
        Vbid vbid,
        const CompactionConfig& c,
        const CookieIface* ck,
        std::chrono::milliseconds delay) {
    return cb::engine_errc::not_supported;
}

cb::engine_errc EphemeralBucket::cancelCompaction(Vbid vbid) {
    return cb::engine_errc::not_supported;
}

VBucketPtr EphemeralBucket::makeVBucket(
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
    (void)hlcEpochSeqno; // Ephemeral overrides this to be 0
    (void)maxVisibleSeqno; // Ephemeral overrides this to be 0
    // Not using make_shared or allocate_shared
    // 1. make_shared doesn't accept a Deleter
    // 2. allocate_shared has inconsistencies between platforms in calling
    //    alloc.destroy (libc++ doesn't call it)
    auto* vb = new EphemeralVBucket(id,
                                    state,
                                    stats,
                                    engine.getCheckpointConfig(),
                                    shard,
                                    lastSeqno,
                                    lastSnapStart,
                                    lastSnapEnd,
                                    std::move(table),
                                    std::move(newSeqnoCb),
                                    makeSyncWriteResolvedCB(),
                                    makeSyncWriteCompleteCB(),
                                    makeSeqnoAckCB(),
                                    makeCheckpointDisposer(),
                                    engine.getConfiguration(),
                                    eviction_policy,
                                    std::move(manifest),
                                    this,
                                    initState,
                                    purgeSeqno,
                                    maxCas,
                                    mightContainXattrs,
                                    replicationTopology);

    vb->ht.setMemChangedCallback([vb, &stats = stats](int64_t delta) {
        if (vb->getState() == vbucket_state_replica) {
            stats.replicaHTMemory += delta;
        }
    });
    vb->checkpointManager->setOverheadChangedCallback(
            [vb, &stats = stats](int64_t delta) {
                if (vb->getState() == vbucket_state_replica) {
                    stats.replicaCheckpointOverhead += delta;
                }
            });
    return VBucketPtr(vb, VBucket::DeferredDeleter(engine));
}

void EphemeralBucket::completeStatsVKey(const CookieIface* cookie,
                                        const DocKey& key,
                                        Vbid vbid,
                                        uint64_t bySeqNum) {
    throw std::logic_error(
            "EphemeralBucket::completeStatsVKey() "
            "is not a valid call. Called on " +
            vbid.to_string() + " for key: " +
            std::string(reinterpret_cast<const char*>(key.data()), key.size()));
}

RollbackResult EphemeralBucket::doRollback(Vbid vbid, uint64_t rollbackSeqno) {
    /* For now we always rollback to zero */
    return RollbackResult(
            /* not a success as we would rather reset vb */ false);
}

void EphemeralBucket::enableTombstonePurgerTask() {
    ExecutorPool::get()->cancel(tombstonePurgerTask->getId());
    scheduleTombstonePurgerTask();
}

void EphemeralBucket::scheduleTombstonePurgerTask() {
    ExecutorPool::get()->schedule(tombstonePurgerTask);
}

void EphemeralBucket::disableTombstonePurgerTask() {
    ExecutorPool::get()->cancel(tombstonePurgerTask->getId());
}

void EphemeralBucket::reconfigureForEphemeral(Configuration& config) {
    // Disable access scanner - we never create it anyway, but set to
    // disabled as to not mislead the user via stats.
    config.setAccessScannerEnabled(false);
    // Disable Bloom filter - it is currently no use for us (both
    // alive+deleted keys are kept in HashTable).
    config.setBfilterEnabled(false);
}

void EphemeralBucket::notifyNewSeqno(const Vbid vbid,
                                     const VBNotifyCtx& notifyCtx) {
    if (notifyCtx.notifyFlusher) {
        notifyFlusher(vbid);
    }
    if (notifyCtx.notifyReplication) {
        notifyReplication(vbid, notifyCtx.bySeqno, notifyCtx.syncWrite);
    }

    /* In ephemeral buckets we must notify high priority requests as well.
       We do not wait for persistence to notify high priority requests */
    VBucketPtr vb = getVBucket(vbid);
    if (vb) {
        auto toNotify = vb->getHighPriorityNotifications(
                engine, notifyCtx.bySeqno, HighPriorityVBNotify::Seqno);

        if (!toNotify.empty() && notifyHpReqTask) {
            notifyHpReqTask->wakeup(std::move(toNotify));
        }
    }
}

// Protected methods //////////////////////////////////////////////////////////

std::unique_ptr<VBucketCountVisitor> EphemeralBucket::makeVBCountVisitor(
        vbucket_state_t state) {
    return std::make_unique<EphemeralVBucket::CountVisitor>(state);
}

void EphemeralBucket::appendAggregatedVBucketStats(
        const VBucketCountVisitor& active,
        const VBucketCountVisitor& replica,
        const VBucketCountVisitor& pending,
        const VBucketCountVisitor& dead,
        const BucketStatCollector& collector) {
    // The CountVisitors passed in are expected to all be Ephemeral subclasses.
    auto& ephActive = dynamic_cast<const EphemeralVBucket::CountVisitor&>(active);
    auto& ephReplica = dynamic_cast<const EphemeralVBucket::CountVisitor&>(replica);
    auto& ephPending = dynamic_cast<const EphemeralVBucket::CountVisitor&>(pending);

    // Add stats for the base class:
    KVBucket::appendAggregatedVBucketStats(
            active, replica, pending, dead, collector);

    // Add Ephemeral-specific stats - add stats for each of active, replica
    // and pending vBuckets.

    for (const auto& visitor : {ephActive, ephReplica, ephPending}) {
        auto state = VBucket::toString(visitor.getVBucketState());
        auto stateCol = collector.withLabels({{"state", state}});

        using namespace cb::stats;

        stateCol.addStat(Key::vb_auto_delete_count, visitor.autoDeleteCount);
        stateCol.addStat(Key::vb_ht_tombstone_purged_count,
                         visitor.htDeletedPurgeCount);
        stateCol.addStat(Key::vb_seqlist_count, visitor.seqlistCount);
        stateCol.addStat(Key::vb_seqlist_deleted_count,
                         visitor.seqlistDeletedCount);
        stateCol.addStat(Key::vb_seqlist_purged_count,
                         visitor.seqListPurgeCount);
        stateCol.addStat(Key::vb_seqlist_read_range_count,
                         visitor.seqlistReadRangeCount);
        stateCol.addStat(Key::vb_seqlist_stale_count,
                         visitor.seqlistStaleCount);
        stateCol.addStat(Key::vb_seqlist_stale_value_bytes,
                         visitor.seqlistStaleValueBytes);
        stateCol.addStat(Key::vb_seqlist_stale_metadata_bytes,
                         visitor.seqlistStaleMetadataBytes);
    }

#undef ARP_STAT
}

bool EphemeralBucket::isValidBucketDurabilityLevel(
        cb::durability::Level level) const {
    switch (level) {
    case cb::durability::Level::None:
    case cb::durability::Level::Majority:
        return true;
    case cb::durability::Level::MajorityAndPersistOnMaster:
    case cb::durability::Level::PersistToMajority:
        // No persistence on Ephemeral
        return false;
    }
    folly::assume_unreachable();
}

EphemeralBucket::NotifyHighPriorityReqTask::NotifyHighPriorityReqTask(
        EventuallyPersistentEngine& e)
    : GlobalTask(&e,
                 TaskId::NotifyHighPriorityReqTask,
                 std::numeric_limits<int>::max(),
                 false) {
}

bool EphemeralBucket::NotifyHighPriorityReqTask::run() {
    std::map<const CookieIface*, cb::engine_errc> notifyQ;
    {
        /* It is necessary that the toNotifyLock is not held while
           actually notifying. */
        std::lock_guard<std::mutex> lg(toNotifyLock);
        std::swap(notifyQ, toNotify);
    }

    for (auto& notify : notifyQ) {
        EP_LOG_INFO("{} for cookie :{} and status {}",
                    getDescription(),
                    static_cast<const void*>(notify.first),
                    notify.second);
        engine->notifyIOComplete(notify.first, notify.second);
    }

    /* Lets assume that the task will be explicitly woken */
    snooze(std::numeric_limits<int>::max());

    /* But, also check if another thread already tried to wake up the task */
    bool scheduleSoon = false;
    {
        std::lock_guard<std::mutex> lg(toNotifyLock);
        if (!toNotify.empty()) {
            scheduleSoon = true;
        }
    }

    if (scheduleSoon) {
        /* Good to call snooze without holding toNotifyLock */
        snooze(0);
    }

    /* Run the task again after snoozing */
    return true;
}

std::string EphemeralBucket::NotifyHighPriorityReqTask::getDescription() const {
    return "Ephemeral: Notify HighPriority Request";
}

std::chrono::microseconds
EphemeralBucket::NotifyHighPriorityReqTask::maxExpectedDuration() const {
    // Typical (p50) duration is under 50us; however a long tail has been
    // observed (p99.9 of 1s). Set initially to 1s; consider reducing
    // when source of slowness has been identified.
    return std::chrono::seconds(1);
}

void EphemeralBucket::NotifyHighPriorityReqTask::wakeup(
        std::map<const CookieIface*, cb::engine_errc> notifies) {
    {
        /* Add the connections to be notified */
        std::lock_guard<std::mutex> lg(toNotifyLock);
        toNotify.insert(make_move_iterator(begin(notifies)),
                        make_move_iterator(end(notifies)));
    }

    /* wake up the task */
    ExecutorPool::get()->wake(getId());
}

bool EphemeralBucket::maybeScheduleManifestPersistence(
        const CookieIface* cookie,
        std::unique_ptr<Collections::Manifest>& newManifest) {
    return false; // newManifest not taken
}
