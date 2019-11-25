/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2016 Couchbase, Inc.
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

#include "ephemeral_bucket.h"

#include "bucket_logger.h"
#include "ep_engine.h"
#include "ep_types.h"
#include "ephemeral_tombstone_purger.h"
#include "ephemeral_vb.h"
#include "ephemeral_vb_count_visitor.h"
#include "executorpool.h"
#include "failover-table.h"
#include "replicationthrottle.h"
#include "rollback_result.h"
#include "statwriter.h"

#include <platform/sized_buffer.h>

/**
 * A configuration value changed listener that responds to Ephemeral bucket
 * parameter changes.
 */
class EphemeralValueChangedListener : public ValueChangedListener {
public:
    EphemeralValueChangedListener(EphemeralBucket& bucket)
        : bucket(bucket) {
    }

    void stringValueChanged(const std::string& key,
                            const char* value) override {
        if (key == "ephemeral_full_policy") {
            if (cb::const_char_buffer(value) == "auto_delete") {
                bucket.enableItemPager();
            } else if (cb::const_char_buffer(value) == "fail_new_data") {
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

ENGINE_ERROR_CODE EphemeralBucket::scheduleCompaction(Vbid vbid,
                                                      const CompactionConfig& c,
                                                      const void* ck) {
    return ENGINE_ENOTSUP;
}

ENGINE_ERROR_CODE EphemeralBucket::cancelCompaction(Vbid vbid) {
    return ENGINE_ENOTSUP;
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
        const nlohmann::json& replicationTopology,
        uint64_t maxVisibleSeqno) {
    (void)hlcEpochSeqno; // Ephemeral overrides this to be 0
    (void)maxVisibleSeqno; // Ephemeral overrides this to be 0
    // Not using make_shared or allocate_shared
    // 1. make_shared doesn't accept a Deleter
    // 2. allocate_shared has inconsistencies between platforms in calling
    //    alloc.destroy (libc++ doesn't call it)
    return VBucketPtr(new EphemeralVBucket(id,
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
                                           engine.getConfiguration(),
                                           eviction_policy,
                                           std::move(manifest),
                                           initState,
                                           purgeSeqno,
                                           maxCas,
                                           mightContainXattrs,
                                           replicationTopology),
                      VBucket::DeferredDeleter(engine));
}

void EphemeralBucket::completeStatsVKey(const void* cookie,
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

        if (toNotify.size() && notifyHpReqTask) {
            notifyHpReqTask->wakeup(std::move(toNotify));
        }
    }
}

// Protected methods //////////////////////////////////////////////////////////

std::unique_ptr<VBucketCountVisitor> EphemeralBucket::makeVBCountVisitor(
        vbucket_state_t state) {
    return std::make_unique<EphemeralVBucket::CountVisitor>(state);
}

void EphemeralBucket::appendAggregatedVBucketStats(VBucketCountVisitor& active,
                                                   VBucketCountVisitor& replica,
                                                   VBucketCountVisitor& pending,
                                                   VBucketCountVisitor& dead,
                                                   const void* cookie,
                                                   const AddStatFn& add_stat) {
    // The CountVisitors passed in are expected to all be Ephemeral subclasses.
    auto& ephActive = dynamic_cast<EphemeralVBucket::CountVisitor&>(active);
    auto& ephReplica = dynamic_cast<EphemeralVBucket::CountVisitor&>(replica);
    auto& ephPending = dynamic_cast<EphemeralVBucket::CountVisitor&>(pending);

    // Add stats for the base class:
    KVBucket::appendAggregatedVBucketStats(
            active, replica, pending, dead, cookie, add_stat);

    // Add Ephemeral-specific stats - add stats for each of active, replica
    // and pending vBuckets.

#define ARP_STAT(k, v)                                                    \
    do {                                                                  \
        add_casted_stat("vb_active_" k, ephActive.v, add_stat, cookie);   \
        add_casted_stat("vb_replica_" k, ephReplica.v, add_stat, cookie); \
        add_casted_stat("vb_pending_" k, ephPending.v, add_stat, cookie); \
    } while (0)

    ARP_STAT("auto_delete_count", autoDeleteCount);
    ARP_STAT("ht_tombstone_purged_count", htDeletedPurgeCount);
    ARP_STAT("seqlist_count", seqlistCount);
    ARP_STAT("seqlist_deleted_count", seqlistDeletedCount);
    ARP_STAT("seqlist_purged_count", seqListPurgeCount);
    ARP_STAT("seqlist_read_range_count", seqlistReadRangeCount);
    ARP_STAT("seqlist_stale_count", seqlistStaleCount);
    ARP_STAT("seqlist_stale_value_bytes", seqlistStaleValueBytes);
    ARP_STAT("seqlist_stale_metadata_bytes", seqlistStaleMetadataBytes);

#undef ARP_STAT
}

EphemeralBucket::NotifyHighPriorityReqTask::NotifyHighPriorityReqTask(
        EventuallyPersistentEngine& e)
    : GlobalTask(&e,
                 TaskId::NotifyHighPriorityReqTask,
                 std::numeric_limits<int>::max(),
                 false) {
}

bool EphemeralBucket::NotifyHighPriorityReqTask::run() {
    std::map<const void*, ENGINE_ERROR_CODE> notifyQ;
    {
        /* It is necessary that the toNotifyLock is not held while
           actually notifying. */
        std::lock_guard<std::mutex> lg(toNotifyLock);
        notifyQ = std::move(toNotify);
    }

    for (auto& notify : notifyQ) {
        EP_LOG_INFO("{} for cookie :{} and status {}",
                    getDescription(),
                    notify.first,
                    notify.second);
        engine->notifyIOComplete(notify.first, notify.second);
    }

    /* Lets assume that the task will be explicitly woken */
    snooze(std::numeric_limits<int>::max());

    /* But, also check if another thread already tried to wake up the task */
    bool scheduleSoon = false;
    {
        std::lock_guard<std::mutex> lg(toNotifyLock);
        if (toNotify.size()) {
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

std::string
EphemeralBucket::NotifyHighPriorityReqTask::getDescription() {
    return "Ephemeral: Notify HighPriority Request";
}

std::chrono::microseconds
EphemeralBucket::NotifyHighPriorityReqTask::maxExpectedDuration() {
    // Typical (p50) duration is under 50us; however a long tail has been
    // observed (p99.9 of 1s). Set initially to 1s; consider reducing
    // when source of slowness has been identified.
    return std::chrono::seconds(1);
}

void EphemeralBucket::NotifyHighPriorityReqTask::wakeup(
        std::map<const void*, ENGINE_ERROR_CODE> notifies) {
    {
        /* Add the connections to be notified */
        std::lock_guard<std::mutex> lg(toNotifyLock);
        toNotify.insert(make_move_iterator(begin(notifies)),
                        make_move_iterator(end(notifies)));
    }

    /* wake up the task */
    ExecutorPool::get()->wake(getId());
}
