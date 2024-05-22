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

#include "magma-kvstore_config.h"

#include "configuration.h"
#include "error_handler.h"
#include "magma-kvstore.h"
#include "magma-kvstore_fs.h"

#include <memcached/server_core_iface.h>

/// A listener class to update MagmaKVSTore related configs at runtime.
class MagmaKVStoreConfig::ConfigChangeListener : public ValueChangedListener {
public:
    ConfigChangeListener(MagmaKVStoreConfig& c) : config(c) {
    }

    void sizeValueChanged(std::string_view key, size_t value) override {
        if (key == "magma_fragmentation_percentage") {
            config.setMagmaFragmentationPercentage(value);
        } else if (key == "magma_flusher_thread_percentage") {
            config.setMagmaFlusherThreadPercentage(value);
        } else if (key == "persistent_metadata_purge_age") {
            config.setMetadataPurgeAge(value);
        } else if (key == "magma_seq_tree_data_block_size") {
            config.setMagmaSeqTreeDataBlockSize(value);
        } else if (key == "magma_seq_tree_index_block_size") {
            config.setMagmaSeqTreeIndexBlockSize(value);
        } else if (key == "magma_key_tree_data_block_size") {
            config.setMagmaKeyTreeDataBlockSize(value);
        } else if (key == "magma_key_tree_index_block_size") {
            config.setMagmaKeyTreeIndexBlockSize(value);
        } else if (key == "magma_fusion_cache_size") {
            config.setMagmaFusionCacheSize(value);
        }
    }

    void floatValueChanged(std::string_view key, float value) override {
        if (key == "magma_mem_quota_ratio") {
            config.setMagmaMemQuotaRatio(value);
        }
    }

    void stringValueChanged(std::string_view key, const char* value) override {
        if (key == "vbucket_mapping_sanity_checking_error_mode") {
            config.setVBucketMappingErrorHandlingMethod(
                    cb::getErrorHandlingMethod(value));
        }
    }

    void booleanValueChanged(std::string_view key, bool b) override {
        if (key == "vbucket_mapping_sanity_checking") {
            config.setSanityCheckVBucketMapping(b);
        } else if (key == "magma_enable_block_cache") {
            config.setMagmaEnableBlockCache(b);
        } else if (key == "magma_per_document_compression_enabled") {
            config.setPerDocumentCompressionEnabled(b);
        }
    }

private:
    MagmaKVStoreConfig& config;
};

MagmaKVStoreConfig::MagmaKVStoreConfig(Configuration& config,
                                       std::string_view backend,
                                       uint16_t numShards,
                                       uint16_t shardid)
    : KVStoreConfig(config, backend, numShards, shardid) {
    bucketQuota = config.getMaxSize();
    magmaMemoryQuotaLowWaterMarkRatio =
            config.getMagmaMemQuotaLowWatermarkRatio();
    magmaDeleteMemtableWritecache = config.getMagmaDeleteMemtableWritecache();
    magmaDeleteFragRatio = config.getMagmaDeleteFragRatio();
    magmaMaxCheckpoints = config.getMagmaMaxCheckpoints();
    magmaCheckpointInterval =
            std::chrono::milliseconds(1s * config.getMagmaCheckpointInterval());
    magmaMinCheckpointInterval = std::chrono::milliseconds(
            1s * config.getMagmaMinCheckpointInterval());
    magmaCheckpointThreshold = config.getMagmaCheckpointThreshold();
    magmaHeartbeatInterval =
            std::chrono::milliseconds(1s * config.getMagmaHeartbeatInterval());
    magmaMemQuotaRatio = config.getMagmaMemQuotaRatio();
    magmaWriteCacheRatio = config.getMagmaWriteCacheRatio();
    magmaMaxWriteCache = config.getMagmaMaxWriteCache();
    magmaEnableDirectIo = config.isMagmaEnableDirectIo();
    magmaInitialWalBufferSize = config.getMagmaInitialWalBufferSize();
    magmaSyncEveryBatch = config.isMagmaSyncEveryBatch();
    magmaEnableUpsert = config.isMagmaEnableUpsert();
    magmaExpiryFragThreshold = config.getMagmaExpiryFragThreshold();
    magmaExpiryPurgerInterval =
            std::chrono::seconds(config.getMagmaExpiryPurgerInterval());
    magmaEnableBlockCache = config.isMagmaEnableBlockCache();
    magmaFragmentationPercentage = config.getMagmaFragmentationPercentage();
    magmaFlusherPercentage = config.getMagmaFlusherThreadPercentage();
    magmaMaxRecoveryBytes = config.getMagmaMaxRecoveryBytes();
    magmaMaxLevel0TTL =
            std::chrono::seconds(1s * config.getMagmaMaxLevel0Ttl());
    magmaMaxDefaultStorageThreads = config.getMagmaMaxDefaultStorageThreads();
    magmaMinValueBlockSizeThreshold =
            config.getMagmaMinValueBlockSizeThreshold();
    metadataPurgeAge = config.getPersistentMetadataPurgeAge();
    magmaBloomFilterAccuracy = config.getMagmaBloomFilterAccuracy();
    magmaBloomFilterAccuracyForBottomLevel =
            config.getMagmaBloomFilterAccuracyForBottomLevel();
    magmaEnableWAL = config.isMagmaEnableWal();
    magmaEnableMemoryOptimizedWrites =
            config.isMagmaEnableMemoryOptimizedWrites();
    magmaEnableGroupCommit = config.isMagmaEnableGroupCommit();
    magmaGroupCommitMaxSyncWaitDuration = std::chrono::milliseconds(
            config.getMagmaGroupCommitMaxSyncWaitDurationMs());
    magmaGroupCommitMaxTransactionCount =
            config.getMagmaGroupCommitMaxTransactionCount();
    magmaIndexCompressionAlgo = config.getMagmaIndexCompressionAlgo();
    magmaDataCompressionAlgo = config.getMagmaDataCompressionAlgo();
    perDocumentCompressionEnabled =
            config.isMagmaPerDocumentCompressionEnabled();
    magmaSeqTreeDataBlockSize = config.getMagmaSeqTreeDataBlockSize();
    magmaSeqTreeIndexBlockSize = config.getMagmaSeqTreeIndexBlockSize();
    magmaKeyTreeDataBlockSize = config.getMagmaKeyTreeDataBlockSize();
    magmaKeyTreeIndexBlockSize = config.getMagmaKeyTreeIndexBlockSize();
    magmaCfg.FS = getMagmaTrackingFileSystem(magmaCfg.GetDefaultFS());

    config.addValueChangedListener(
            "magma_enable_block_cache",
            std::make_unique<ConfigChangeListener>(*this));
    config.addValueChangedListener(
            "magma_fragmentation_percentage",
            std::make_unique<ConfigChangeListener>(*this));
    config.addValueChangedListener(
            "magma_flusher_thread_percentage",
            std::make_unique<ConfigChangeListener>(*this));
    config.addValueChangedListener(
            "persistent_metadata_purge_age",
            std::make_unique<ConfigChangeListener>(*this));
    config.addValueChangedListener(
            "magma_mem_quota_ratio",
            std::make_unique<ConfigChangeListener>(*this));
    config.addValueChangedListener(
            "magma_seq_tree_data_block_size",
            std::make_unique<ConfigChangeListener>(*this));
    config.addValueChangedListener(
            "magma_seq_tree_index_block_size",
            std::make_unique<ConfigChangeListener>(*this));
    config.addValueChangedListener(
            "magma_key_tree_data_block_size",
            std::make_unique<ConfigChangeListener>(*this));
    config.addValueChangedListener(
            "magma_key_tree_index_block_size",
            std::make_unique<ConfigChangeListener>(*this));

    sanityCheckVBucketMapping = config.isVbucketMappingSanityChecking();
    vBucketMappingErrorHandlingMethod = cb::getErrorHandlingMethod(
            config.getVbucketMappingSanityCheckingErrorMode());

    config.addValueChangedListener(
            "vbucket_mapping_sanity_checking",
            std::make_unique<ConfigChangeListener>(*this));
    config.addValueChangedListener(
            "vbucket_mapping_sanity_checking_error_mode",
            std::make_unique<ConfigChangeListener>(*this));

    config.addValueChangedListener(
            "magma_per_document_compression_enabled",
            std::make_unique<ConfigChangeListener>(*this));

    historyRetentionTime =
            std::chrono::seconds(config.getHistoryRetentionSeconds());
    historyRetentionSize = config.getHistoryRetentionBytes();

    fusionEndpointURI = config.getMagmaFusionEndpointUri();
    fusionVolumeName = config.getMagmaFusionVolumeName();

    config.addValueChangedListener(
            "magma_fusion_cache_size",
            std::make_unique<ConfigChangeListener>(*this));
    fusionCacheSize = config.getMagmaFusionCacheSize();

    fusionVolatileStoragePath = config.getMagmaFusionVolatileStoragePath();
}

void MagmaKVStoreConfig::setStore(MagmaKVStore* store) {
    this->store = store;
}

void MagmaKVStoreConfig::setMagmaFragmentationPercentage(size_t value) {
    magmaFragmentationPercentage.store(value);
    if (store) {
        store->setMagmaFragmentationPercentage(value);
    }
}

void MagmaKVStoreConfig::setStorageThreads(
        ThreadPoolConfig::StorageThreadCount value) {
    storageThreads.store(value);
    if (store) {
        store->calculateAndSetMagmaThreads();
    }
}

void MagmaKVStoreConfig::setMagmaFlusherThreadPercentage(size_t value) {
    magmaFlusherPercentage.store(value);
    if (store) {
        store->calculateAndSetMagmaThreads();
    }
}

void MagmaKVStoreConfig::setBucketQuota(size_t value) {
    // Just update the cached value, the update to magma is driven via the
    // engine.
    bucketQuota.store(value);
}

void MagmaKVStoreConfig::setMagmaMemQuotaRatio(float value) {
    magmaMemQuotaRatio.store(value);
    if (store) {
        store->setMaxDataSize(bucketQuota);
    }
}

void MagmaKVStoreConfig::setMagmaEnableBlockCache(bool enable) {
    magmaEnableBlockCache.store(enable);
    if (store) {
        store->setMagmaEnableBlockCache(enable);
    }
}

void MagmaKVStoreConfig::setMagmaSeqTreeDataBlockSize(size_t value) {
    magmaSeqTreeDataBlockSize.store(value);
    if (store) {
        store->setMagmaSeqTreeDataBlockSize(value);
    }
}

void MagmaKVStoreConfig::setMagmaSeqTreeIndexBlockSize(size_t value) {
    magmaSeqTreeIndexBlockSize.store(value);
    if (store) {
        store->setMagmaSeqTreeIndexBlockSize(value);
    }
}

void MagmaKVStoreConfig::setMagmaKeyTreeDataBlockSize(size_t value) {
    magmaKeyTreeDataBlockSize.store(value);
    if (store) {
        store->setMagmaKeyTreeDataBlockSize(value);
    }
}

void MagmaKVStoreConfig::setMagmaKeyTreeIndexBlockSize(size_t value) {
    magmaKeyTreeIndexBlockSize.store(value);
    if (store) {
        store->setMagmaKeyTreeIndexBlockSize(value);
    }
}

void MagmaKVStoreConfig::setMagmaFusionCacheSize(size_t bytes) {
    fusionCacheSize = bytes;
    if (store) {
        store->setFusionCacheSize(bytes);
    }
}

void MagmaKVStoreConfig::setMakeDirectoryFn(magma::DirectoryConstructor fn) {
    Expects(getenv("MEMCACHED_UNIT_TESTS") != nullptr);
    magmaCfg.FS.MakeDirectory = fn;
}

void MagmaKVStoreConfig::setReadOnly(bool readOnly) {
    setReadOnlyHook();
    magmaCfg.ReadOnly = readOnly;
    magmaCfg.FS = getMagmaTrackingFileSystem(magmaCfg.GetDefaultFS());
}
