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
#pragma once

#include "kv_bucket.h"

/* Forward declarations */
class RollbackResult;

/**
 * Ephemeral Bucket
 *
 * A bucket type without any persistent data storage. Similar to memcache (default)
 * buckets, except with VBucket goodness - replication, rebalance, failover.
 */

class EphemeralBucket : public KVBucket {
public:
    explicit EphemeralBucket(EventuallyPersistentEngine& theEngine);

    ~EphemeralBucket() override;

    bool initialize() override;

    ENGINE_ERROR_CODE scheduleCompaction(Vbid vbid,
                                         const CompactionConfig& c,
                                         const void* ck) override;

    ENGINE_ERROR_CODE cancelCompaction(Vbid vbid) override;

    /// Eviction not supported for Ephemeral buckets - without some backing
    /// storage, there is nowhere to evict /to/.
    cb::mcbp::Status evictKey(const DocKey& key,
                              Vbid vbucket,
                              const char** msg) override {
        return cb::mcbp::Status::NotSupported;
    }

    /// File stats not supported for Ephemeral buckets.
    ENGINE_ERROR_CODE getFileStats(
            const BucketStatCollector& collector) override {
        return ENGINE_KEY_ENOENT;
    }

    /// Disk stats not supported for Ephemeral buckets.
    ENGINE_ERROR_CODE getPerVBucketDiskStats(
            const void* cookie, const AddStatFn& add_stat) override {
        return ENGINE_KEY_ENOENT;
    }

    void attemptToFreeMemory() override;

    /**
     * Creates an EphemeralVBucket
     */
    VBucketPtr makeVBucket(Vbid id,
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
                           uint64_t maxVisibleSeqno) override;

    /// Do nothing - no flusher to notify
    void notifyFlusher(const Vbid vbid) override {
    }

    ENGINE_ERROR_CODE statsVKey(const DocKey& key,
                                Vbid vbucket,
                                const void* cookie) override {
        return ENGINE_ENOTSUP;
    }

    void completeStatsVKey(const void* cookie,
                           const DocKey& key,
                           Vbid vbid,
                           uint64_t bySeqNum) override;

    RollbackResult doRollback(Vbid vbid, uint64_t rollbackSeqno) override;

    void rollbackUnpersistedItems(VBucket& vb, int64_t rollbackSeqno) override {
        // No op
    }

    LoadPreparedSyncWritesResult loadPreparedSyncWrites(
            folly::SharedMutex::WriteHolder& vbStateLh, VBucket& vb) override {
        // No op, return 0 prepares loaded
        return {0, 0};
    }

    void notifyNewSeqno(const Vbid vbid, const VBNotifyCtx& notifyCtx) override;

    /**
     * Enables the Ephemeral Tombstone purger task (if not already enabled).
     * This runs periodically, and based on memory pressure.
     */
    void enableTombstonePurgerTask();

    /**
     * Request that the Ephemeral Tombstone purger task is scheduled to run
     */
    void scheduleTombstonePurgerTask();

    /**
     * Disables the Ephemeral Tombstone purger task (if enabled).
     */
    void disableTombstonePurgerTask();

    bool isGetAllKeysSupported() const override {
        return false;
    }

    bool isByIdScanSupported() const override {
        return false;
    }

    bool maybeScheduleManifestPersistence(
            const void* cookie,
            std::unique_ptr<Collections::Manifest>& newManifest) override;

    // Static methods /////////////////////////////////////////////////////////

    /** Apply necessary modifications to the Configuration for an Ephemeral
     *  bucket (e.g. disable features which are not applicable).
     * @param config Configuration to modify.
     */
    static void reconfigureForEphemeral(Configuration& config);

    bool canEvictFromReplicas() override {
        return false;
    }

protected:
    std::unique_ptr<VBucketCountVisitor> makeVBCountVisitor(
            vbucket_state_t state) override;

    void appendAggregatedVBucketStats(
            VBucketCountVisitor& active,
            VBucketCountVisitor& replica,
            VBucketCountVisitor& pending,
            VBucketCountVisitor& dead,
            const BucketStatCollector& collector) override;

    bool isValidBucketDurabilityLevel(
            cb::durability::Level level) const override;

    // Protected member variables /////////////////////////////////////////////

    /// Task responsible for purging in-memory tombstones.
    ExTask tombstonePurgerTask;

private:
    /**
     * Task responsible for notifying high priority requests (usually during
     * rebalance)
     */
    class NotifyHighPriorityReqTask : public GlobalTask {
    public:
        explicit NotifyHighPriorityReqTask(EventuallyPersistentEngine& e);

        bool run() override;

        std::string getDescription() override;

        std::chrono::microseconds maxExpectedDuration() override;

        /**
         * Adds the connections to be notified by the task and then wakes up
         * the task.
         *
         * @param notifies Map of connections to be notified
         */
        void wakeup(std::map<const void*, ENGINE_ERROR_CODE> notifies);

    private:
        /* All the notifications to be called by the task */
        std::map<const void*, ENGINE_ERROR_CODE> toNotify;

        /* Serialize access to write/read of toNotify */
        std::mutex toNotifyLock;
    };

    // Private member variables ///////////////////////////////////////////////
    std::shared_ptr<NotifyHighPriorityReqTask> notifyHpReqTask;
};
