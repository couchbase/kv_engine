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

#include "ep_engine.h"
#include "ep_types.h"
#include "ephemeral_tombstone_purger.h"
#include "ephemeral_vb.h"
#include "ephemeral_vb_count_visitor.h"
#include "failover-table.h"

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
                LOG(EXTENSION_LOG_WARNING,
                    "EphemeralValueChangedListener: Invalid value '%s' for "
                    "'ephemeral_full_policy - ignoring.",
                    value);
            }
        } else {
            LOG(EXTENSION_LOG_WARNING,
                "EphemeralValueChangedListener: Failed to change value for "
                "unknown key '%s'",
                key.c_str());
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
            LOG(EXTENSION_LOG_WARNING,
                "EphemeralValueChangedListener: Failed to change value for "
                "unknown key '%s'",
                key.c_str());
        }
    }

    void sizeValueChanged(const std::string& key, size_t value) override {
        if (key == "ephemeral_metadata_purge_interval") {
            // Cancel and re-schedule the task to pick up the new interval.
            bucket.enableTombstonePurgerTask();
        } else {
            LOG(EXTENSION_LOG_WARNING,
                "EphemeralValueChangedListener: Failed to change value for "
                "unknown key '%s'",
                key.c_str());
        }
    }

private:
    EphemeralBucket& bucket;
};

EphemeralBucket::EphemeralBucket(EventuallyPersistentEngine& theEngine)
    : KVBucket(theEngine),
      notifyHpReqTask(make_STRCPtr<NotifyHighPriorityReqTask>(theEngine)) {
    /* We always have VALUE_ONLY eviction policy because a key not
       present in HashTable implies key not present at all.
       Note: This should not be confused with the eviction algorithm
             that we are going to use like NRU, FIFO etc. */
    eviction_policy = VALUE_ONLY;
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
    itemPagerTask = new ItemPager(&engine, stats);
    if (config.getEphemeralFullPolicy() == "auto_delete") {
        enableItemPager();
    }
    engine.getConfiguration().addValueChangedListener(
            "ephemeral_full_policy", new EphemeralValueChangedListener(*this));

    // Tombstone purger - scheduled periodically as long as we have a
    // non-zero interval. Can be dynamically adjusted, so add config listeners.
    tombstonePurgerTask = new EphTombstonePurgerTask(&engine, stats);
    auto interval = config.getEphemeralMetadataPurgeInterval();
    if (interval > 0) {
        enableTombstonePurgerTask();
    }
    config.addValueChangedListener("ephemeral_metadata_purge_age",
                                   new EphemeralValueChangedListener(*this));
    config.addValueChangedListener("ephemeral_metadata_purge_interval",
                                   new EphemeralValueChangedListener(*this));

    // High priority vbucket request notification task
    ExecutorPool::get()->schedule(notifyHpReqTask);

    return true;
}

VBucketPtr EphemeralBucket::makeVBucket(
        VBucket::id_type id,
        vbucket_state_t state,
        KVShard* shard,
        std::unique_ptr<FailoverTable> table,
        NewSeqnoCallback newSeqnoCb,
        vbucket_state_t initState,
        int64_t lastSeqno,
        uint64_t lastSnapStart,
        uint64_t lastSnapEnd,
        uint64_t purgeSeqno,
        uint64_t maxCas,
        const std::string& collectionsManifest) {
    return std::make_shared<EphemeralVBucket>(id,
                                              state,
                                              stats,
                                              engine.getCheckpointConfig(),
                                              shard,
                                              lastSeqno,
                                              lastSnapStart,
                                              lastSnapEnd,
                                              std::move(table),
                                              std::move(newSeqnoCb),
                                              engine.getConfiguration(),
                                              eviction_policy,
                                              initState,
                                              purgeSeqno,
                                              maxCas,
                                              collectionsManifest);
}

void EphemeralBucket::completeStatsVKey(const void* cookie,
                                        const DocKey& key,
                                        uint16_t vbid,
                                        uint64_t bySeqNum) {
    throw std::logic_error(
            "EphemeralBucket::completeStatsVKey() "
            "is not a valid call. Called on vb " +
            std::to_string(vbid) + "for key: " +
            std::string(reinterpret_cast<const char*>(key.data()), key.size()));
}

RollbackResult EphemeralBucket::doRollback(uint16_t vbid,
                                           uint64_t rollbackSeqno) {
    /* For now we always rollback to zero */
    return RollbackResult(/* not a success as we would rather reset vb */ false,
                          /* highSeqno */ 0,
                          /* snapStartSeqno */ 0,
                          /* snapEndSeqno */ 0);
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
    // Disable warmup - it is not applicable to Ephemeral buckets.
    config.setWarmup(false);
    // Disable TAP - not supported for Ephemeral.
    config.setTap(false);

    // In-memory backfilling is currently not memory managed. Therefore set the
    // scan buffer (per backfill buffer for managing backfill memory usage)
    // byte limit, item limit to max. Otherwise, this may result in incomplete
    // DCP backfills which may inturn cause hang or data loss in DCP clients
    // like rebalance, xdcr etc.
    config.setDcpScanByteLimit(std::numeric_limits<size_t>::max());
    config.setDcpScanItemLimit(std::numeric_limits<size_t>::max());

    // for the same reason as above set overall DCP backfill limit to max
    config.setDcpBackfillByteLimit(std::numeric_limits<size_t>::max());
}

size_t EphemeralBucket::getNumPersistedDeletes(uint16_t vbid) {
    /* the name is getNumPersistedDeletes, in ephemeral buckets the equivalent
       meaning is the number of deletes seen by the vbucket.
       This is needed by ns-server during vb-takeover */
    VBucketPtr vb = getVBucket(vbid);
    return vb->getNumInMemoryDeletes();
}

void EphemeralBucket::notifyNewSeqno(const uint16_t vbid,
                                     const VBNotifyCtx& notifyCtx) {
    if (notifyCtx.notifyFlusher) {
        notifyFlusher(vbid);
    }
    if (notifyCtx.notifyReplication) {
        notifyReplication(vbid, notifyCtx.bySeqno);
    }

    /* In ephemeral buckets we must notify high priority requests as well.
       We do not wait for persistence to notify high priority requests */
    VBucketPtr vb = getVBucket(vbid);

    auto toNotify = vb->getHighPriorityNotifications(
            engine, notifyCtx.bySeqno, HighPriorityVBNotify::Seqno);

    if (toNotify.size() && notifyHpReqTask) {
        notifyHpReqTask->wakeup(std::move(toNotify));
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
                                                   ADD_STAT add_stat) {

    // The CountVisitors passed in are expected to all be Ephemeral subclasses.
    auto& ephActive = dynamic_cast<EphemeralVBucket::CountVisitor&>(active);
    auto& ephReplica = dynamic_cast<EphemeralVBucket::CountVisitor&>(replica);
    auto& ephPending = dynamic_cast<EphemeralVBucket::CountVisitor&>(pending);

    // Add stats for the base class:
    KVBucket::appendAggregatedVBucketStats(
            active, replica, pending, dead, cookie, add_stat);

    // Add Ephemeral-specific stats.

#define DO_STAT(k, v)                            \
    do {                                         \
        add_casted_stat(k, v, add_stat, cookie); \
    } while (0)

    // Active vBuckets:
    DO_STAT("vb_active_auto_delete_count", ephActive.autoDeleteCount);
    DO_STAT("vb_active_seqlist_count", ephActive.seqlistCount);
    DO_STAT("vb_active_seqlist_deleted_count", ephActive.seqlistDeletedCount);
    DO_STAT("vb_active_seqlist_read_range_count",
            ephActive.seqlistReadRangeCount);
    DO_STAT("vb_active_seqlist_stale_count", ephActive.seqlistStaleCount);
    DO_STAT("vb_active_seqlist_stale_value_bytes",
            ephActive.seqlistStaleValueBytes);
    DO_STAT("vb_active_seqlist_stale_metadata_bytes",
            ephActive.seqlistStaleMetadataBytes);

    // Replica vBuckets:
    DO_STAT("vb_replica_auto_delete_count", ephReplica.autoDeleteCount);
    DO_STAT("vb_replica_seqlist_count", ephReplica.seqlistCount);
    DO_STAT("vb_replica_seqlist_deleted_count", ephReplica.seqlistDeletedCount);
    DO_STAT("vb_replica_seqlist_read_range_count",
            ephReplica.seqlistReadRangeCount);
    DO_STAT("vb_replica_seqlist_stale_count", ephReplica.seqlistStaleCount);
    DO_STAT("vb_replica_seqlist_stale_value_bytes",
            ephReplica.seqlistStaleValueBytes);
    DO_STAT("vb_replica_seqlist_stale_metadata_bytes",
            ephReplica.seqlistStaleMetadataBytes);

    // Pending vBuckets:
    DO_STAT("vb_pending_auto_delete_count", ephPending.autoDeleteCount);
    DO_STAT("vb_pending_seqlist_count", ephPending.seqlistCount);
    DO_STAT("vb_pending_seqlist_deleted_count", ephPending.seqlistDeletedCount);
    DO_STAT("vb_pending_seqlist_read_range_count",
            ephPending.seqlistReadRangeCount);
    DO_STAT("vb_pending_seqlist_stale_count", ephPending.seqlistStaleCount);
    DO_STAT("vb_pending_seqlist_stale_value_bytes",
            ephPending.seqlistStaleValueBytes);
    DO_STAT("vb_pending_seqlist_stale_metadata_bytes",
            ephPending.seqlistStaleMetadataBytes);
#undef DO_STAT
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
        LOG(EXTENSION_LOG_NOTICE,
            "%s for cookie :%p and status %d",
            to_string(getDescription()).c_str(),
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

cb::const_char_buffer
EphemeralBucket::NotifyHighPriorityReqTask::getDescription() {
    return "Ephemeral: Notify HighPriority Request";
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
