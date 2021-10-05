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

#pragma once

#include "kvstore/kvstore_config.h"

#include "error_handler.h"
#include "libmagma/magma.h"
#include "utilities/testing_hook.h"

#include <memcached/thread_pool_config.h>
#include <chrono>

using namespace std::chrono_literals;

class Configuration;
class MagmaKVStore;

// This class represents the MagmaKVStore specific configuration.
// MagmaKVStore uses this in place of the KVStoreConfig base class.
class MagmaKVStoreConfig : public KVStoreConfig {
public:
    // Initialize the object from the central EPEngine Configuration
    MagmaKVStoreConfig(Configuration& config,
                       std::string_view backend,
                       uint16_t numShards,
                       uint16_t shardid);

    void setStore(MagmaKVStore* store);

    size_t getBucketQuota() const {
        return bucketQuota;
    }
    void setBucketQuota(size_t value);

    size_t getMagmaDeleteMemtableWritecache() const {
        return magmaDeleteMemtableWritecache;
    }
    float getMagmaDeleteFragRatio() const {
        return magmaDeleteFragRatio;
    }
    size_t getMagmaMaxCheckpoints() const {
        return magmaMaxCheckpoints;
    }
    std::chrono::milliseconds getMagmaCheckpointInterval() const {
        return magmaCheckpointInterval;
    }
    void setMagmaCheckpointInterval(std::chrono::milliseconds val) {
        magmaCheckpointInterval = val;
    }
    float getMagmaCheckpointThreshold() const {
        return magmaCheckpointThreshold;
    }
    std::chrono::milliseconds getMagmaHeartbeatInterval() const {
        return magmaHeartbeatInterval;
    }
    size_t getMagmaValueSeparationSize() const {
        return magmaValueSeparationSize;
    }
    size_t getMagmaMaxWriteCache() const {
        return magmaMaxWriteCache;
    }
    float getMagmaMemQuotaRatio() const {
        return magmaMemQuotaRatio;
    }
    float getMagmaWriteCacheRatio() const {
        return magmaWriteCacheRatio;
    }
    bool getMagmaEnableDirectIo() const {
        return magmaEnableDirectIo;
    }
    size_t getMagmaInitialWalBufferSize() const {
        return magmaInitialWalBufferSize;
    }

    bool getMagmaSyncEveryBatch() const {
        return magmaSyncEveryBatch;
    }
    void setMagmaSyncEveryBatch(bool value) {
        magmaSyncEveryBatch = value;
    }

    bool getMagmaEnableUpsert() const {
        return magmaEnableUpsert;
    }
    float getMagmaExpiryFragThreshold() const {
        return magmaExpiryFragThreshold;
    }
    std::chrono::seconds getMagmaExpiryPurgerInterval() const {
        return magmaExpiryPurgerInterval;
    }
    bool getMagmaEnableBlockCache() const {
        return magmaEnableBlockCache;
    }
    size_t getMagmaFragmentationPercentage() const {
        return magmaFragmentationPercentage.load();
    }
    void setMagmaFragmentationPercentage(size_t value);

    void setStorageThreads(ThreadPoolConfig::StorageThreadCount value);

    ThreadPoolConfig::StorageThreadCount getStorageThreads() const {
        return storageThreads.load();
    }

    size_t getMagmaFlusherPercentage() const {
        return magmaFlusherPercentage.load();
    }
    void setMagmaFlusherThreadPercentage(size_t value);

    size_t getMagmaMaxDefaultStorageThreads() const {
        return magmaMaxDefaultStorageThreads;
    }
    size_t getMagmaMaxRecoveryBytes() const {
        return magmaMaxRecoveryBytes;
    }

    void setMetadataPurgeAge(size_t value) {
        metadataPurgeAge.store(value);
    }

    size_t getMetadataPurgeAge() const {
        return metadataPurgeAge.load();
    }

    std::chrono::seconds getMagmaMaxLevel0TTL() const {
        return magmaMaxLevel0TTL;
    }

    float getMagmaBloomFilterAccuracy() const {
        return magmaBloomFilterAccuracy;
    }

    float getMagmaBloomFilterAccuracyForBottomLevel() const {
        return magmaBloomFilterAccuracyForBottomLevel;
    }

    bool getMagmaEnableWAL() const {
        return magmaEnableWAL;
    }

    bool getMagmaEnableGroupCommit() const {
        return magmaEnableGroupCommit;
    }

    std::chrono::milliseconds getMagmaGroupCommitMaxSyncWaitDuration() const {
        return magmaGroupCommitMaxSyncWaitDuration;
    }

    size_t getMagmaGroupCommitMaxTransactionCount() const {
        return magmaGroupCommitMaxTransactionCount;
    }

    void setMagmaMemQuotaRatio(float value);

    void setMakeDirectoryFn(magma::DirectoryConstructor fn) {
        magmaCfg.FS.MakeDirectory = fn;
    }

    void setReadOnly(bool readOnly) {
        setReadOnlyHook();
        magmaCfg.ReadOnly = readOnly;
    }

    bool isSanityCheckingVBucketMapping() const {
        return sanityCheckVBucketMapping;
    }
    void setSanityCheckVBucketMapping(bool value) {
        sanityCheckVBucketMapping.store(value);
    }

    cb::ErrorHandlingMethod getVBucketMappingErrorHandlingMethod() const {
        return vBucketMappingErrorHandlingMethod;
    }
    void setVBucketMappingErrorHandlingMethod(cb::ErrorHandlingMethod value) {
        vBucketMappingErrorHandlingMethod = value;
    }

    magma::Magma::Config magmaCfg;

    /**
     * Called when we attempt to set the config to read only (so that we can
     * make config adjustments before we open the read only magma instance)
     */
    TestingHook<> setReadOnlyHook;

private:
    class ConfigChangeListener;

    MagmaKVStore* store;

    // Bucket RAM Quota
    std::atomic<size_t> bucketQuota;

    // Magma uses a lazy update model to maintain the sequence index. It
    // maintains a list of deleted seq #s that were deleted from the key Index.
    size_t magmaDeleteMemtableWritecache;

    // Magma compaction runs frequently and applies all methods of compaction
    // (removal of duplicates, expiry, tombstone removal) but it does not always
    // visit every sstable. In order to run compaction over less visited
    // sstables, magma uses a variety of methods to determine which range of
    // sstables need visited.
    //
    // This is the minimum fragmentation ratio for when magma will trigger
    // compaction based on the number of duplicate keys removed.
    float magmaDeleteFragRatio;

    // Magma keeps track of expiry histograms per sstable to determine
    // when an expiry compaction should be run. The fragmentation threshold
    // applies across all the kvstore but only specific sstables will be
    // visited.
    float magmaExpiryFragThreshold;

    // Intervals at which magma expiry purger is executed
    std::chrono::seconds magmaExpiryPurgerInterval;

    // Max checkpoints that can be rolled back to
    int magmaMaxCheckpoints;

    // Time interval between checkpoints
    std::chrono::milliseconds magmaCheckpointInterval;

    // Fraction of total data before checkpoint is created
    float magmaCheckpointThreshold;

    // Time interval (in milliseconds) between heartbeat tasks
    std::chrono::milliseconds magmaHeartbeatInterval;

    // Magma minimum value for key value separation.
    // Values < magmaValueSeparationSize, value remains in key index.
    size_t magmaValueSeparationSize;

    // WAL ensures Magma's atomicity, durability. Disabling it is useful in
    // performance analysis.
    bool magmaEnableWAL;

    // Magma uses a common skiplist to buffer all items at the shard level
    // called the write cache. The write cache contains items from all the
    // kvstores that are part of the shard and when it is flushed, each
    // kvstore will receive a few items each.
    size_t magmaMaxWriteCache;

    // Magma Memory Quota as a ratio of Bucket Quota
    std::atomic<float> magmaMemQuotaRatio;

    // Magma uses a write ahead log to quickly persist items during bg
    // flushing. This buffer contains the items along with control records
    // like begin/end transaction. It can be flushed many times for a batch
    // of items.
    size_t magmaInitialWalBufferSize;

    // Used in testing to make sure each batch is flushed to disk to simulate
    // how couchstore flushes each batch to disk.
    bool magmaSyncEveryBatch;

    // When true, the kv_engine will utilize Magma's upsert capabiltiy
    // but accurate document counts for the data store or collections can
    // not be maintained.
    bool magmaEnableUpsert;

    // Ratio of available memory that magma write cache can utilized up
    // to the magmaMaxWriteCache limit.
    float magmaWriteCacheRatio;

    // When true, directs magma to use direct io when writing sstables.
    bool magmaEnableDirectIo;

    // Magma can utilize an LRU policy driven block cache that maintains
    // the index blocks from sstables.
    bool magmaEnableBlockCache;

    // Percentage of fragmentation which magma will attempt to maintain via
    // compaction. Atomic as this can be changed dynamically.
    std::atomic<size_t> magmaFragmentationPercentage;

    // Percentage of storage threads which magma will use a flushers. The
    // remaining threads will be compactors. Atomic as this can be changed
    // dynamically.
    std::atomic<size_t> magmaFlusherPercentage;

    /**
     * Number of threads the storage backend is allowed to run. The "default"
     * value of 0 infers the number of storage threads from the number of writer
     * threads. This value exists in the memcached config, not the bucket
     * config, so we have to default the value here for unit tests.
     */
    std::atomic<ThreadPoolConfig::StorageThreadCount> storageThreads{
            ThreadPoolConfig::StorageThreadCount::Default};

    /**
     * If the number of storage threads = 0, then we set the number
     * of storage threads based on the number of writer threads up to a
     * maximum of 20 threads and use magma_flusher_thread_percentage to
     * determine the ratio of flusher and compactor threads.
     */
    size_t magmaMaxDefaultStorageThreads{20};

    /**
     * Cached copy of the persistent_metadata_purge_age. Used in
     * MagmaKVStore::getExpiryOrPurgeTime() to calculate the time at which
     * tombstones should be purged.
     */
    std::atomic<size_t> metadataPurgeAge;

    /**
     * Max amount of data that is replayed from the WAL during magma's
     * recovery. When this threshold is reached, magma creates a temporary
     * checkpoint to recover at. This is per kvstore and in bytes.
     */
    size_t magmaMaxRecoveryBytes{67108864};

    /**
     * Maximum life time for data in level-0 before it is
     * merged
     */
    std::chrono::seconds magmaMaxLevel0TTL{600};

    float magmaBloomFilterAccuracy;
    float magmaBloomFilterAccuracyForBottomLevel;

    /**
     * Group Commit allows transactions in magma to be grouped
     * together to reduce the number of WAL fsyncs. When a
     * transaction is ready to fsync, if there are new transactions
     * waiting to start, we stall the transaction waiting to fsync
     * until there are no more transactions waiting to start for
     * a given magma instance.
     */
    bool magmaEnableGroupCommit;

    /**
     * When group commit is enabled, magma_group_commit_max_sync_wait_duration
     * can be used as a limit to how long a stalled transaction will wait
     * before the WAL fsync is enabled regardless if there are
     * transactions waiting to execute. If the oldest transaction waiting
     * has been waint for magmaGroupCommitMaxSyncWaitDuration or longer,
     * the current transaction will trigger the WAL fsync.
     */
    std::chrono::milliseconds magmaGroupCommitMaxSyncWaitDuration;

    /**
     * When group commit is enabled, magma_group_commit_max_transaction_count
     * can be used as a limit to how long a stalled transaction will wait
     * before the WAL fsync is enabled regardless if there are
     * transactions waiting to execute. If the current transaction plus the
     * count of waiting transactions >= magmaGroupCommitMaxTransactionCount,
     * the current transaction will trigger the WAL fsync.
     */
    size_t magmaGroupCommitMaxTransactionCount;

    /**
     * Should we validate that the key - vBucket mapping is correct?
     */
    std::atomic_bool sanityCheckVBucketMapping;

    /**
     * The method in which errors are handled should the key - vBucket mapping
     * be incorrect.
     */
    std::atomic<cb::ErrorHandlingMethod> vBucketMappingErrorHandlingMethod;
};
