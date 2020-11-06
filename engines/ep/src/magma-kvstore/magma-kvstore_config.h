/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2018 Couchbase, Inc
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

#include "kvstore_config.h"
#include "libmagma/magma.h"
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
                       uint16_t numShards,
                       uint16_t shardid);

    void setStore(MagmaKVStore* store);

    size_t getBucketQuota() {
        return bucketQuota;
    }
    size_t getMagmaDeleteMemtableWritecache() const {
        return magmaDeleteMemtableWritecache;
    }
    float getMagmaDeleteFragRatio() const {
        return magmaDeleteFragRatio;
    }
    size_t getMagmaMaxCommitPoints() const {
        return magmaMaxCommitPoints;
    }
    size_t getMagmaMaxCheckpoints() const {
        return magmaMaxCheckpoints;
    }
    size_t getMagmaCommitPointInterval() const {
        return magmaCommitPointInterval;
    }
    std::chrono::milliseconds getMagmaCheckpointInterval() const {
        return magmaCheckpointInterval;
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
    bool getMagmaCheckpointEveryBatch() const {
        return magmaCheckpointEveryBatch;
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

    void setStorageThreads(size_t value);

    size_t getStorageThreads() const {
        return storageThreads.load();
    }

    size_t getMagmaFlusherPercentage() const {
        return magmaFlusherPercentage.load();
    }
    void setMagmaFlusherThreadPercentage(size_t value);

    size_t getNumWriterThreads() const {
        return numWriterThreads.load();
    }
    void setNumWriterThreads(size_t value);

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

    magma::Magma::Config magmaCfg;

private:
    class ConfigChangeListener;

    MagmaKVStore* store;

    // Bucket RAM Quota
    size_t bucketQuota;

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

    // Max commit points that can be rolled back to
    int magmaMaxCommitPoints;

    // Max checkpoints that can be rolled back to
    int magmaMaxCheckpoints;

    // Time interval (in minutes) between commit points
    size_t magmaCommitPointInterval;

    // Time interval between checkpoints
    std::chrono::milliseconds magmaCheckpointInterval;

    // Fraction of total data before checkpoint is created
    float magmaCheckpointThreshold;

    // Time interval (in milliseconds) between heartbeat tasks
    std::chrono::milliseconds magmaHeartbeatInterval;

    // Magma minimum value for key value separation.
    // Values < magmaValueSeparationSize, value remains in key index.
    size_t magmaValueSeparationSize;

    // Magma uses a common skiplist to buffer all items at the shard level
    // called the write cache. The write cache contains items from all the
    // kvstores that are part of the shard and when it is flushed, each
    // kvstore will receive a few items each.
    size_t magmaMaxWriteCache;

    // Magma Memory Quota as a ratio of Bucket Quota
    float magmaMemQuotaRatio;

    // Magma uses a write ahead log to quickly persist items during bg
    // flushing. This buffer contains the items along with control records
    // like begin/end transaction. It can be flushed many times for a batch
    // of items.
    size_t magmaInitialWalBufferSize;

    // Used in testing to make sure each batch is flushed to disk to simulate
    // how couchstore flushes each batch to disk.
    bool magmaCheckpointEveryBatch;

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

    // Number of KV writer threads. Used to calculate how many storage threads
    // to create for magma in the default case.
    std::atomic<size_t> numWriterThreads;

    /**
     * Number of threads the storage backend is allowed to run. The "default"
     * value of 0 infers the number of storage threads from the number of writer
     * threads. This value exists in the memcached config, not the bucket
     * config, so we have to default the value here for unit tests.
     */
    std::atomic<size_t> storageThreads{0};

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
};
