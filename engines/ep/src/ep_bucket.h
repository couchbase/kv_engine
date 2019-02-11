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

    void reset() override;

    /**
     * Flushes all items waiting for persistence in a given vbucket
     * @param vbid The id of the vbucket to flush
     * @return A pair of {moreToFlush, flushCount}:
     *         moreToFlush - true if there are still items remaining for this
     *         vBucket.
     *         flushCount - the number of items flushed.
     */
    std::pair<bool, size_t> flushVBucket(Vbid vbid);

    /**
     * Set the number of flusher items which can be included in a
     * single flusher commit - more than this number of items will split
     * into multiple commits.
     */
    void setFlusherBatchSplitTrigger(size_t limit);

    void commit(KVStore& kvstore, Collections::VB::Flush& collectionsFlush);

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
                           bool mightContainXattrs) override;

    ENGINE_ERROR_CODE statsVKey(const DocKey& key,
                                Vbid vbucket,
                                const void* cookie) override;

    void completeStatsVKey(const void* cookie,
                           const DocKey& key,
                           Vbid vbid,
                           uint64_t bySeqNum) override;

    RollbackResult doRollback(Vbid vbid, uint64_t rollbackSeqno) override;

    void rollbackUnpersistedItems(VBucket& vb, int64_t rollbackSeqno) override;

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
     * Method checks with Warmup if a setVBState should block.
     * On returning true, Warmup will have saved the cookie ready for
     * IO notify complete.
     * If there's no Warmup returns false
     * @param cookie the callers cookie for later notification.
     * @return true if setVBState should return EWOULDBLOCK
     */
    bool shouldSetVBStateBlock(const void* cookie) override;

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

protected:
    // During the warmup phase we might want to enable external traffic
    // at a given point in time.. The LoadStorageKvPairCallback will be
    // triggered whenever we want to check if we could enable traffic..
    friend class LoadStorageKVPairCallback;

    class ValueChangedListener;

    void flushOneDeleteAll();

    std::unique_ptr<PersistenceCallback> flushOneDelOrSet(const queued_item& qi,
                                                          VBucketPtr& vb);

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
    void dropKey(Vbid vbid, const DocKey& key, int64_t bySeqno);

    /**
     * Max number of backill items in a single flusher batch before we split
     * into multiple batches.
     */
    size_t flusherBatchSplitTrigger;

    /**
     * Indicates whether erroneous tombstones need to retained or not during
     * compaction
     */
    Couchbase::RelaxedAtomic<bool> retainErroneousTombstones;

    std::unique_ptr<Warmup> warmupTask;
};
