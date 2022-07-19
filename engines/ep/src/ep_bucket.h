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

/**
 * Eventually Persistent Bucket
 *
 * A bucket type which stores modifications to disk asynchronously
 * ("eventually").
 * Uses hash partitioning of the keyspace into VBuckets, to support
 * replication, rebalance, failover.
 */

class EPBucket : public KVBucket {
public:
    EPBucket(EventuallyPersistentEngine& theEngine);

    ~EPBucket() override;

    bool initialize() override;

    void deinitialize() override;

    enum class MoreAvailable : uint8_t { No = 0, Yes };
    enum class WakeCkptRemover : uint8_t { No = 0, Yes };

    struct FlushResult {
        FlushResult(MoreAvailable m, size_t n, WakeCkptRemover w)
            : moreAvailable(m), numFlushed(n), wakeupCkptRemover(w) {
        }

        bool operator==(const FlushResult& other) const {
            return (moreAvailable == other.moreAvailable &&
                    numFlushed == other.numFlushed &&
                    wakeupCkptRemover == other.wakeupCkptRemover);
        }

        MoreAvailable moreAvailable = MoreAvailable::No;
        size_t numFlushed = 0;
        WakeCkptRemover wakeupCkptRemover = WakeCkptRemover::No;
    };

    /**
     * Flushes all items waiting for persistence in a given vbucket
     * @param vbid The id of the vbucket to flush
     * @return an instance of FlushResult
     */
    FlushResult flushVBucket(Vbid vbid);

    /**
     * Set the number of flusher items which can be included in a
     * single flusher commit - more than this number of items will split
     * into multiple commits.
     */
    void setFlusherBatchSplitTrigger(size_t limit);

    /**
     * Persist whatever flush-batch previously queued into KVStore.
     *
     * @param vbid
     * @param kvstore
     * @param [out] collectionsFlush
     * @return true if flush succeeds, false otherwise
     */
    bool commit(Vbid vbid,
                KVStore& kvstore,
                Collections::VB::Flush& collectionsFlush);

    /// Start the Flusher for all shards in this bucket.
    void startFlusher();

    /// Stop the Flusher for all shards in this bucket.
    void stopFlusher();

    bool pauseFlusher() override;
    bool resumeFlusher() override;

    void wakeUpFlusher() override;

    /**
     * Starts the background fetcher for each shard.
     * @return true if successful.
     */
    bool startBgFetcher();

    /// Stops the background fetcher for each shard.
    void stopBgFetcher();

    ENGINE_ERROR_CODE scheduleCompaction(Vbid vbid,
                                         const CompactionConfig& c,
                                         const void* ck) override;

    ENGINE_ERROR_CODE cancelCompaction(Vbid vbid) override;

    /**
     * Compaction of a database file
     *
     * @param ctx Context for compaction hooks
     * @param ck cookie used to notify connection of operation completion
     *
     * return true if the compaction needs to be rescheduled and false
     *             otherwise
     */
    bool doCompact(const CompactionConfig& config,
                   uint64_t purgeSeq,
                   const void* cookie);

    std::pair<uint64_t, bool> getLastPersistedCheckpointId(Vbid vb) override;

    ENGINE_ERROR_CODE getFileStats(const void* cookie,
                                   const AddStatFn& add_stat) override;

    ENGINE_ERROR_CODE getPerVBucketDiskStats(
            const void* cookie, const AddStatFn& add_stat) override;

    size_t getPageableMemCurrent() const override;
    size_t getPageableMemHighWatermark() const override;
    size_t getPageableMemLowWatermark() const override;

    /**
     * Creates a VBucket object from warmup (can set collection state)
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
                           const nlohmann::json& replicationTopology,
                           uint64_t maxVisibleSeqno) override;

    ENGINE_ERROR_CODE statsVKey(const DocKey& key,
                                Vbid vbucket,
                                const void* cookie) override;

    void completeStatsVKey(const void* cookie,
                           const DocKey& key,
                           Vbid vbid,
                           uint64_t bySeqNum) override;

    RollbackResult doRollback(Vbid vbid, uint64_t rollbackSeqno) override;

    void rollbackUnpersistedItems(VBucket& vb, int64_t rollbackSeqno) override;

    LoadPreparedSyncWritesResult loadPreparedSyncWrites(
            folly::SharedMutex::WriteHolder& vbStateLh, VBucket& vb) override;

    /**
     * Returns the ValueFilter to use for KVStore scans, given the bucket
     * compression mode.
     */
    ValueFilter getValueFilterForCompressionMode();

    void notifyNewSeqno(const Vbid vbid, const VBNotifyCtx& notifyCtx) override;

    virtual bool isGetAllKeysSupported() const override {
        return true;
    }

    void setRetainErroneousTombstones(bool value) {
        retainErroneousTombstones = value;
    }

    bool isRetainErroneousTombstones() const {
        return retainErroneousTombstones.load();
    }

    Warmup* getWarmup(void) const override;

    bool isWarmingUp() override;

    bool isWarmupOOMFailure() override;

    /**
     * This method store the given cookie for later notification iff Warmup has
     * yet to reach and complete the PopulateVBucketMap phase.
     *
     * @param cookie the callers cookie which might be stored for later
     *        notification (see return value)
     * @return true if the cookie was stored for later notification, false if
     *         not.
     */
    bool maybeWaitForVBucketWarmup(const void* cookie) override;

    /**
     * Creates a warmup task if the engine configuration has "warmup=true"
     */
    void initializeWarmupTask();

    /**
     * Starts the warmup task if one is present
     */
    void startWarmupTask();

    bool maybeEnableTraffic();

    void warmupCompleted();

    void releaseBlockedCookies() override;

    bool canEvictFromReplicas() override {
        return true;
    }

protected:
    // During the warmup phase we might want to enable external traffic
    // at a given point in time.. The LoadStorageKvPairCallback will be
    // triggered whenever we want to check if we could enable traffic..
    friend class LoadStorageKVPairCallback;

    class ValueChangedListener;

    void flushOneDelOrSet(const queued_item& qi, VBucketPtr& vb);

    /**
     * Compaction of a database file
     *
     * @param config the configuration to use for running compaction
     */
    void compactInternal(const CompactionConfig& config, uint64_t purgeSeqno);

    /**
     * Remove completed compaction tasks or wake snoozed tasks
     *
     * @param db_file_id vbucket id for couchstore
     */
    void updateCompactionTasks(Vbid db_file_id);

    void stopWarmup();

    /// function which is passed down to compactor for dropping keys
    void dropKey(Vbid vbid, const DiskDocKey& key, int64_t bySeqno);

    /**
     * @todo MB-37858: legacy from TAP, remove.
     * Probably used only by ns_server in the old days of TAP, should be the
     * TAP-equivalent of the current DCP SeqnoPersistence. Must be confirmed.
     * It uses the CM::pCursorPreCheckpointId for inferring what is the last
     * complete checkpoint persisted.
     * Note that currently CM::pCursorPreCheckpointId is used only by
     * CheckpointPersistence and some checkpoint-removal logic that does not
     * need it strictly, so we can remove that too when we resolve MB-37858.
     *
     * @param vb
     */
    void handleCheckpointPersistence(VBucket& vb) const;

    /**
     * Performs operations that must be performed after flush succeeds,
     * regardless of whether we flush non-meta items or a new vbstate only.
     *
     * @param vb
     * @param flushStart Used for updating stats
     * @param itemsFlushed Used for updating stats
     * @param aggStats Used for updating stats
     * @param collectionFlush Used for performing collection-related operations
     */
    void flushSuccessEpilogue(
            VBucket& vb,
            const std::chrono::steady_clock::time_point flushStart,
            size_t itemsFlushed,
            const VBucket::AggregatedFlushStats& aggStats,
            const Collections::VB::Flush& collectionFlush);

    bool isValidBucketDurabilityLevel(
            cb::durability::Level level) const override;

    /**
     * Max number of backill items in a single flusher batch before we split
     * into multiple batches.
     * Atomic as can be changed by ValueChangedListener on one thread and read
     * by flusher on other thread.
     */
    std::atomic<size_t> flusherBatchSplitTrigger;

    /**
     * Indicates whether erroneous tombstones need to retained or not during
     * compaction
     */
    cb::RelaxedAtomic<bool> retainErroneousTombstones;

    std::unique_ptr<Warmup> warmupTask;
};

/**
 * Callback for notifying flusher about pending mutations.
 */
class NotifyFlusherCB : public Callback<Vbid> {
public:
    NotifyFlusherCB(KVShard* sh) : shard(sh) {
    }

    void callback(Vbid& vb) override;

private:
    KVShard* shard;
};
