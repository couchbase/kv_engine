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
                                   ADD_STAT add_stat) override;

    ENGINE_ERROR_CODE getPerVBucketDiskStats(const void* cookie,
                                             ADD_STAT add_stat) override;
    /**
     * Creates a VBucket object.
     */
    VBucketPtr makeVBucket(Vbid id,
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
                           int64_t hlcEpochSeqno,
                           bool mightContainXattrs,
                           const std::string& collectionsManifest) override;

    ENGINE_ERROR_CODE statsVKey(const DocKey& key,
                                Vbid vbucket,
                                const void* cookie) override;

    void completeStatsVKey(const void* cookie,
                           const DocKey& key,
                           Vbid vbid,
                           uint64_t bySeqNum) override;

    RollbackResult doRollback(Vbid vbid, uint64_t rollbackSeqno) override;

    void rollbackUnpersistedItems(VBucket& vb, int64_t rollbackSeqno) override;

    size_t getNumPersistedDeletes(Vbid vbid) override {
        return getROUnderlying(vbid)->getNumPersistedDeletes(vbid);
    }

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

protected:
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
};
