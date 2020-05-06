/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2017 Couchbase, Inc
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

#include "kvstore_config.h"

#include "bucket_logger.h"
#include "environment.h"

#include <memory>
#include <utility>

/// A listener class to update KVStore related configs at runtime.
class KVStoreConfig::ConfigChangeListener : public ValueChangedListener {
public:
    explicit ConfigChangeListener(KVStoreConfig& c) : config(c) {
    }

    void sizeValueChanged(const std::string& key, size_t value) override {
        if (key == "fsync_after_every_n_bytes_written") {
            config.setPeriodicSyncBytes(value);
        } else if (key == "pitr_max_history_age") {
            config.setPitrMaxHistoryAge(std::chrono::seconds{value});
        } else if (key == "pitr_granularity") {
            config.setPitrGranularity(std::chrono::seconds{value});
        }
    }

    void booleanValueChanged(const std::string& key, bool b) override {
        if (key == "pitr_enabled") {
            config.setPitrEnabled(b);
        }
    }

private:
    KVStoreConfig& config;
};

KVStoreConfig::KVStoreConfig(Configuration& config,
                             uint16_t numShards,
                             uint16_t shardid)
    : KVStoreConfig(config.getMaxVbuckets(),
                    numShards,
                    config.getDbname(),
                    config.getBackend(),
                    shardid) {
    setPeriodicSyncBytes(config.getFsyncAfterEveryNBytesWritten());
    config.addValueChangedListener(
            "fsync_after_every_n_bytes_written",
            std::make_unique<ConfigChangeListener>(*this));

    setPitrEnabled(config.isPitrEnabled());
    setPitrGranularity(std::chrono::seconds{config.getPitrGranularity()});
    setPitrMaxHistoryAge(std::chrono::seconds{config.getPitrMaxHistoryAge()});
    config.addValueChangedListener(
            "pitr_enabled", std::make_unique<ConfigChangeListener>(*this));
    config.addValueChangedListener(
            "pitr_max_history_age",
            std::make_unique<ConfigChangeListener>(*this));
    config.addValueChangedListener(
            "pitr_granularity", std::make_unique<ConfigChangeListener>(*this));
}

KVStoreConfig::KVStoreConfig(uint16_t _maxVBuckets,
                             uint16_t _maxShards,
                             std::string _dbname,
                             std::string _backend,
                             uint16_t _shardId)
    : maxVBuckets(_maxVBuckets),
      maxShards(_maxShards),
      dbname(std::move(_dbname)),
      backend(std::move(_backend)),
      shardId(_shardId),
      logger(globalBucketLogger.get()) {
    auto env = Environment::get();
    maxFileDescriptors = env.getMaxBackendFileDescriptors();
}

KVStoreConfig::KVStoreConfig(const KVStoreConfig& other)
    : maxVBuckets(other.maxVBuckets),
      maxShards(other.maxShards),
      dbname(other.dbname),
      backend(other.backend),
      shardId(other.shardId),
      logger(other.logger) {
}

KVStoreConfig::~KVStoreConfig() = default;

KVStoreConfig& KVStoreConfig::setLogger(BucketLogger& _logger) {
    logger = &_logger;
    return *this;
}
