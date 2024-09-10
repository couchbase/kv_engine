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

#include "ep_types.h"
#include "libmagma/magma.h"
#include "utilities/testing_hook.h"

#include <memcached/thread_pool_config.h>
#include <chrono>

using namespace std::chrono_literals;

class Configuration;
class FileOpsTracker;
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

    /// The FileOpsTracker the created MagmaKVStore will use.
    void setFileOpsTracker(FileOpsTracker& instance) {
        fileOpsTracker = &instance;
    }

    size_t getBucketQuota() const {
        return bucketQuota;
    }
    void setBucketQuota(size_t value);

    float getMagmaMemoryQuotaLowWaterMarkRatio() const {
        return magmaMemoryQuotaLowWaterMarkRatio;
    }

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
    std::chrono::milliseconds getMagmaMinCheckpointInterval() const {
        return magmaMinCheckpointInterval;
    }
    void setMagmaMinCheckpointInterval(std::chrono::milliseconds val) {
        magmaMinCheckpointInterval = val;
    }
    float getMagmaCheckpointThreshold() const {
        return magmaCheckpointThreshold;
    }
    std::chrono::milliseconds getMagmaHeartbeatInterval() const {
        return magmaHeartbeatInterval;
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
        return magmaEnableBlockCache.load();
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

    size_t getMagmaMinValueBlockSizeThreshold() const {
        return magmaMinValueBlockSizeThreshold;
    }

    size_t getMagmaSeqTreeDataBlockSize() const {
        return magmaSeqTreeDataBlockSize.load();
    }

    void setMagmaSeqTreeDataBlockSize(size_t value);

    size_t getMagmaSeqTreeIndexBlockSize() const {
        return magmaSeqTreeIndexBlockSize.load();
    }

    void setMagmaSeqTreeIndexBlockSize(size_t value);

    size_t getMagmaKeyTreeDataBlockSize() const {
        return magmaKeyTreeDataBlockSize.load();
    }

    void setMagmaKeyTreeDataBlockSize(size_t value);

    size_t getMagmaKeyTreeIndexBlockSize() const {
        return magmaKeyTreeIndexBlockSize.load();
    }

    void setMagmaKeyTreeIndexBlockSize(size_t value);

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

    bool getMagmaEnableMemoryOptimizedWrites() const {
        return magmaEnableMemoryOptimizedWrites;
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

    std::string getMagmaIndexCompressionAlgo() const {
        return magmaIndexCompressionAlgo;
    }

    std::string getMagmaDataCompressionAlgo() const {
        return magmaDataCompressionAlgo;
    }

    void setMagmaMemQuotaRatio(float value);

    void setMagmaEnableBlockCache(bool enable);

    void setMakeDirectoryFn(magma::DirectoryConstructor fn);

    void setReadOnly(bool readOnly);

    bool isReadOnly() const {
        return magmaCfg.ReadOnly;
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

    bool isPerDocumentCompressionEnabled() const {
        return perDocumentCompressionEnabled;
    }
    void setPerDocumentCompressionEnabled(bool value) {
        perDocumentCompressionEnabled = value;
    }

    size_t getHistoryRetentionSize() const {
        return historyRetentionSize;
    }

    std::chrono::seconds getHistoryRetentionTime() const {
        return historyRetentionTime;
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

    /// The tracker that the created Magma KVStore will use.
    FileOpsTracker* fileOpsTracker{nullptr};

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

    // Minimum time interval between two checkpoints
    std::chrono::milliseconds magmaMinCheckpointInterval;

    // Fraction of total data before checkpoint is created
    float magmaCheckpointThreshold;

    // Time interval (in milliseconds) between heartbeat tasks
    std::chrono::milliseconds magmaHeartbeatInterval;

    // WAL ensures Magma's atomicity, durability. Disabling it is useful in
    // performance analysis.
    bool magmaEnableWAL;

    // When enabled, if copying a write batch into memtable results in exceeding
    // the write cache quota, Magma avoids the copy and instead flushes the
    // batch to disk on the writer thread itself. This tradeoffs an increase in
    // write latency for reduced memory consumption and obeys quota limits. If
    // copying a batch keeps us under the quota, Magma will to continue to
    // copy and do the flush in background.
    bool magmaEnableMemoryOptimizedWrites;

    // Magma uses a common skiplist to buffer all items at the shard level
    // called the write cache. The write cache contains items from all the
    // kvstores that are part of the shard and when it is flushed, each
    // kvstore will receive a few items each.
    size_t magmaMaxWriteCache;

    // Magma Memory Quota as a ratio of Bucket Quota
    std::atomic<float> magmaMemQuotaRatio;

    // This ratio is the fraction of memory quota used as Magma's low watermark.
    // The low watermark is used to size the writecache and block cache in
    // magma. This sizing accounts for bloom filter memory usage but bloom
    // filters are not evicted until the memory quota is reached.
    float magmaMemoryQuotaLowWaterMarkRatio;

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
    std::atomic<bool> magmaEnableBlockCache;

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
     * Data block size for SeqIndex SSTable.
     */
    std::atomic<size_t> magmaSeqTreeDataBlockSize{4096};

    /**
     * Index block size for SeqIndex SSTable.
     */
    std::atomic<size_t> magmaSeqTreeIndexBlockSize{4096};

    /**
     * Data block size for KeyIndex SSTable.
     */
    std::atomic<size_t> magmaKeyTreeDataBlockSize{4096};

    /**
     * Index block size for KeyIndex SSTable.
     */
    std::atomic<size_t> magmaKeyTreeIndexBlockSize{4096};

    /**
     * If the number of storage threads = 0, then we set the number
     * of storage threads based on the number of writer threads up to a
     * maximum of 20 threads and use magma_flusher_thread_percentage to
     * determine the ratio of flusher and compactor threads.
     */
    size_t magmaMaxDefaultStorageThreads{20};

    /**
     * Magma creates value blocks for values larger than this size. Value
     * blocks only contain a single KV item and their reads/writes are
     * optimised for memory as it avoids many value copies. Right now
     * compression is turned off for value blocks to reduce memory consumption
     * while building them. This setting should be at least as large as the
     * SeqIndex block size.
     */
    size_t magmaMinValueBlockSizeThreshold;

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

    /**
     * Accuracy of Magma's per SSTable bloom filter. These bloom filters are
     * only enabled for the key index and this config is used for all levels
     * apart from the bottom level. The bloom filter's total size and memory
     * usage per key is computed from this config.
     */
    float magmaBloomFilterAccuracy;
    /**
     * Accuracy of Magma's per SSTable bloom filter. These bloom filters are
     * only enabled for the key index and this config is used only for the
     * bottom level. We have a special config for the bottom since most of the
     * data resides in the bottom level and these bloom filters are only used to
     * avoid IO in case on non-existent keys.
     */
    float magmaBloomFilterAccuracyForBottomLevel;

    /**
     * Index block level and checkpoint file compression algorithm for Magma.
     * This config will be applied during block creation for blocks that don't
     * contain documents ie. blocks that do not reside in the bottom level of
     * the sequence index
     */
    std::string magmaIndexCompressionAlgo;
    /**
     * Compression algorithm used by Magma to compress data blocks where
     * documents are stored.
     */
    std::string magmaDataCompressionAlgo;

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
     * Should the kvstore apply compression to any items which are not already
     * datatype Snappy.
     *
     * Does not affect _block-level_ compression, just per-document compression
     */
    std::atomic<bool> perDocumentCompressionEnabled;

    /**
     * The method in which errors are handled should the key - vBucket mapping
     * be incorrect.
     */
    std::atomic<cb::ErrorHandlingMethod> vBucketMappingErrorHandlingMethod;

    size_t historyRetentionSize{0};
    std::chrono::seconds historyRetentionTime{0};
};
