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
#pragma once

#include "kv_bucket.h"
#include <folly/Synchronized.h>
#include <mcbp/protocol/status.h>
#include <relaxed_atomic.h>

/* Forward declarations */
class RollbackResult;
class EphemeralMemRecovery;

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

    cb::engine_errc scheduleCompaction(
            Vbid vbid,
            const CompactionConfig& c,
            CookieIface* ck,
            std::chrono::milliseconds delay) override;

    cb::engine_errc cancelCompaction(Vbid vbid) override;

    /// Eviction not supported for Ephemeral buckets - without some backing
    /// storage, there is nowhere to evict /to/.
    cb::engine_errc evictKey(const DocKeyView& key,
                             Vbid vbucket,
                             const char** msg) override {
        return cb::engine_errc::not_supported;
    }

    uint64_t getTotalDiskSize() override {
        // Ephemeral buckets do not use a notable amount of disk space.
        return 0;
    }

    /// File stats not supported for Ephemeral buckets.
    cb::engine_errc getFileStats(
            const BucketStatCollector& collector) override {
        return cb::engine_errc::no_such_key;
    }

    cb::engine_errc getImplementationStats(
            const BucketStatCollector& collector) const override;

    /// Disk stats not supported for Ephemeral buckets.
    cb::engine_errc getPerVBucketDiskStats(CookieIface& cookie,
                                           const AddStatFn& add_stat) override {
        return cb::engine_errc::no_such_key;
    }

    void getAggregatedVBucketStats(
            const BucketStatCollector& collector,
            cb::prometheus::MetricGroup metricGroup) override;

    size_t getPageableMemCurrent() const override;
    size_t getPageableMemHighWatermark() const override;
    size_t getPageableMemLowWatermark() const override;

    void attemptToFreeMemory() override;

    /**
     * Creates an EphemeralVBucket
     */
    VBucketPtr makeVBucket(Vbid id,
                           vbucket_state_t state,
                           KVShard* shard,
                           std::unique_ptr<FailoverTable> table,
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
                           uint64_t maxPrepareSeqno) override;

    /// Do nothing - no flusher to notify
    void notifyFlusher(const Vbid vbid) override {
    }

    cb::engine_errc statsVKey(const DocKeyView& key,
                              Vbid vbucket,
                              CookieIface& cookie) override {
        return cb::engine_errc::not_supported;
    }

    void completeStatsVKey(CookieIface& cookie,
                           const DocKeyView& key,
                           Vbid vbid,
                           uint64_t bySeqNum) override;

    std::unique_ptr<RollbackCtx> prepareToRollback(Vbid vbid) override {
        // No op
        return std::make_unique<RollbackCtx>();
    }

    RollbackResult doRollback(Vbid vbid, uint64_t rollbackSeqno) override;

    void rollbackUnpersistedItems(VBucket& vb, int64_t rollbackSeqno) override {
        // No op
    }

    LoadPreparedSyncWritesResult loadPreparedSyncWrites(VBucket& vb) override {
        // No op, return 0 prepares loaded
        return {0, 0, true};
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
            CookieIface* cookie,
            const Collections::Manifest& newManifest) override;

    // Static methods /////////////////////////////////////////////////////////

    /** Apply necessary modifications to the Configuration for an Ephemeral
     *  bucket (e.g. disable features which are not applicable).
     * @param config Configuration to modify.
     */
    static void reconfigureForEphemeral(Configuration& config);

    bool canEvictFromReplicas() override {
        return false;
    }

    Flusher* getOneFlusher() override {
        // No flusher for ephemeral
        return nullptr;
    }


    bool hasPrimaryWarmupLoadedMetaData() override {
        // Nothing to do for warmup, so return true to allow DCP consumers to be
        // made
        return true;
    }

    bool disconnectReplicationAtOOM() const override;

    enum class FullPolicy { AutoDelete, FailNewData };

    FullPolicy getFullPolicy() const {
        return fullPolicy;
    }

    void switchFullPolicy(FullPolicy newPolicy);

    /**
     * @param sleepTime Interval in milliseconds to sleep between runs for
     * EphemeralMemRecovery.
     */
    void setMemRecoveryTaskSleepTime(size_t sleepTime);

    /**
     * Switch between utilising EphemeralMemRecovery or
     * ItemPager (auto-delete only) for memory recovery
     *
     * @param enableEphMemRec If true, use EphemeralMemRecovery
     */
    void useEphemeralMemRecovery(bool enableEphMemRec);

    void doEphemeralMemRecoveryStats(const StatCollector& collector);

protected:
    friend class KVBucketTest;
    friend class SingleThreadedEphemeralTest;

    std::unique_ptr<VBucketCountVisitor> makeVBCountVisitor(
            vbucket_state_t state) override;

    void appendAggregatedVBucketStats(
            const VBucketCountVisitor& active,
            const VBucketCountVisitor& replica,
            const VBucketCountVisitor& pending,
            const VBucketCountVisitor& dead,
            const BucketStatCollector& collector) override;

    bool isValidBucketDurabilityLevel(
            cb::durability::Level level) const override;

    // Protected member variables /////////////////////////////////////////////

    /// Task responsible for purging in-memory tombstones.
    ExTask tombstonePurgerTask;

    /**
     * The policy utilised when the bucket is full.
     *  - auto_delete: Eject/delete items when the bucket is full.
     *  - fail_new_data: Temp-oom when the bucket is full.
     *
     * The ItemPager is enabled if the policy is auto_delete and disabled if
     * the policy is fail_new_data.
     *
     * Writes to this variable are protected by fullPolicyMutex to serialise
     * changes to the policy.
     */
    cb::RelaxedAtomic<FullPolicy> fullPolicy;
    std::mutex fullPolicyMutex;

    /*
     * Task responsible for recovering memory from checkpoint usage when HWM is
     * exceeded. For auto-delete only, the ItemPager is woken up after
     * checkpoint memory recovery has been completed.
     */
    folly::Synchronized<std::shared_ptr<EphemeralMemRecovery>>
            ephemeralMemRecoveryTask;
};
