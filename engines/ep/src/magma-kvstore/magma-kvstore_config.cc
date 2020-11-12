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

#include "magma-kvstore_config.h"

#include "configuration.h"
#include "magma-kvstore.h"

#include <memcached/server_core_iface.h>

/// A listener class to update MagmaKVSTore related configs at runtime.
class MagmaKVStoreConfig::ConfigChangeListener : public ValueChangedListener {
public:
    ConfigChangeListener(MagmaKVStoreConfig& c) : config(c) {
    }

    void sizeValueChanged(const std::string& key, size_t value) override {
        if (key == "magma_fragmentation_percentage") {
            config.setMagmaFragmentationPercentage(value);
        } else if (key == "magma_flusher_thread_percentage") {
            config.setMagmaFlusherThreadPercentage(value);
        } else if (key == "num_writer_threads") {
            config.setNumWriterThreads(value);
        } else if (key == "persistent_metadata_purge_age") {
            config.setMetadataPurgeAge(value);
        }
    }

private:
    MagmaKVStoreConfig& config;
};

MagmaKVStoreConfig::MagmaKVStoreConfig(Configuration& config,
                                       uint16_t numShards,
                                       uint16_t shardid)
    : KVStoreConfig(config, numShards, shardid) {
    bucketQuota = config.getMaxSize();
    magmaDeleteMemtableWritecache = config.getMagmaDeleteMemtableWritecache();
    magmaDeleteFragRatio = config.getMagmaDeleteFragRatio();
    magmaMaxCommitPoints = config.getMagmaMaxCommitPoints();
    magmaCommitPointInterval = config.getMagmaCommitPointInterval();
    magmaValueSeparationSize = config.getMagmaValueSeparationSize();
    magmaMemQuotaRatio = config.getMagmaMemQuotaRatio();
    magmaWriteCacheRatio = config.getMagmaWriteCacheRatio();
    magmaMaxWriteCache = config.getMagmaMaxWriteCache();
    magmaEnableDirectIo = config.isMagmaEnableDirectIo();
    magmaInitialWalBufferSize = config.getMagmaInitialWalBufferSize();
    magmaCommitPointEveryBatch = config.isMagmaCommitPointEveryBatch();
    magmaEnableUpsert = config.isMagmaEnableUpsert();
    magmaExpiryFragThreshold = config.getMagmaExpiryFragThreshold();
    magmaExpiryPurgerInterval =
            std::chrono::seconds(config.getMagmaExpiryPurgerInterval());
    magmaEnableBlockCache = config.isMagmaEnableBlockCache();
    magmaFragmentationPercentage = config.getMagmaFragmentationPercentage();
    magmaFlusherPercentage = config.getMagmaFlusherThreadPercentage();
    magmaMaxDefaultStorageThreads = config.getMagmaMaxDefaultStorageThreads();
    numWriterThreads = config.getNumWriterThreads();
    metadataPurgeAge = config.getPersistentMetadataPurgeAge();

    config.addValueChangedListener(
            "magma_fragmentation_percentage",
            std::make_unique<ConfigChangeListener>(*this));
    config.addValueChangedListener(
            "num_writer_threads",
            std::make_unique<ConfigChangeListener>(*this));
    config.addValueChangedListener(
            "magma_flusher_thread_percentage",
            std::make_unique<ConfigChangeListener>(*this));
    config.addValueChangedListener(
            "persistent_metadata_purge_age",
            std::make_unique<ConfigChangeListener>(*this));
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

void MagmaKVStoreConfig::setStorageThreads(size_t value) {
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

void MagmaKVStoreConfig::setNumWriterThreads(size_t value) {
    numWriterThreads.store(value);
    if (store) {
        store->calculateAndSetMagmaThreads();
    }
}
