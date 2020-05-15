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

#include "magma-kvstore.h"

/// A listener class to update MagmaKVSTore related configs at runtime.
class MagmaKVStoreConfig::ConfigChangeListener : public ValueChangedListener {
public:
    ConfigChangeListener(MagmaKVStoreConfig& c) : config(c) {
    }

    void floatValueChanged(const std::string& key, float value) override {
        if (key == "magma_fragmentation_ratio") {
            config.setMagmaFragmentationRatio(value);
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
    magmaWalBufferSize = config.getMagmaWalBufferSize();
    magmaWalNumBuffers = config.getMagmaWalNumBuffers();
    magmaNumFlushers = config.getMagmaNumFlushers();
    magmaNumCompactors = config.getMagmaNumCompactors();
    magmaCommitPointEveryBatch = config.isMagmaCommitPointEveryBatch();
    magmaEnableUpsert = config.isMagmaEnableUpsert();
    magmaExpiryFragThreshold = config.getMagmaExpiryFragThreshold();
    magmaTombstoneFragThreshold = config.getMagmaTombstoneFragThreshold();
    magmaEnableBlockCache = config.isMagmaEnableBlockCache();
    magmaFragmentationRatio = config.getMagmaFragmentationRatio();

    config.addValueChangedListener(
            "magma_fragmentation_ratio",
            std::make_unique<ConfigChangeListener>(*this));
}

void MagmaKVStoreConfig::setStore(MagmaKVStore* store) {
    this->store = store;
}

void MagmaKVStoreConfig::setMagmaFragmentationRatio(float value) {
    magmaFragmentationRatio.store(value);
    if (store) {
        store->setMagmaFragmentationRatio(value);
    }
}
