/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2017-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "kvstore_config.h"

#include "bucket_logger.h"
#include "configuration.h"
#include "environment.h"
#include "kvstore/couch-kvstore/couch-kvstore-config.h"
#include "kvstore/nexus-kvstore/nexus-kvstore-config.h"

#ifdef EP_USE_MAGMA
#include "kvstore/magma-kvstore/magma-kvstore_config.h"
#endif

#include <memory>
#include <utility>

/// A listener class to update KVStore related configs at runtime.
class KVStoreConfig::ConfigChangeListener : public ValueChangedListener {
public:
    explicit ConfigChangeListener(KVStoreConfig& c) : config(c) {
    }

    void sizeValueChanged(std::string_view key, size_t value) override {
        if (key == "fsync_after_every_n_bytes_written") {
            config.setPeriodicSyncBytes(value);
        } else if (key == "persistent_metadata_purge_age") {
            config.setMetadataPurgeAge(std::chrono::seconds{value});
        }
    }

private:
    KVStoreConfig& config;
};

KVStoreConfig::KVStoreConfig(Configuration& config,
                             std::string_view backend,
                             uint16_t numShards,
                             uint16_t shardid)
    : KVStoreConfig(gsl::narrow_cast<uint16_t>(config.getMaxVbuckets()),
                    numShards,
                    config.getDbname(),
                    backend,
                    shardid) {
    setPeriodicSyncBytes(config.getFsyncAfterEveryNBytesWritten());
    config.addValueChangedListener(
            "fsync_after_every_n_bytes_written",
            std::make_unique<ConfigChangeListener>(*this));

    setMetadataPurgeAge(
            std::chrono::seconds{config.getPersistentMetadataPurgeAge()});
    config.addValueChangedListener(
            "persistent_metadata_purge_age",
            std::make_unique<ConfigChangeListener>(*this));
}

KVStoreConfig::KVStoreConfig(uint16_t _maxVBuckets,
                             uint16_t _maxShards,
                             std::string _dbname,
                             std::string_view _backend,
                             uint16_t _shardId)
    : maxVBuckets(_maxVBuckets),
      maxShards(_maxShards),
      dbname(std::move(_dbname)),
      backend(_backend),
      shardId(_shardId),
      logger(getGlobalBucketLogger().get()) {
    auto& env = Environment::get();
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

size_t KVStoreConfig::getCacheSize() const {
    return std::ceil(float(getMaxVBuckets()) / getMaxShards());
}

std::unique_ptr<KVStoreConfig> KVStoreConfig::createKVStoreConfig(
        Configuration& config,
        std::string_view backend,
        uint16_t numShards,
        uint16_t shardId) {
    std::unique_ptr<KVStoreConfig> kvConfig;
    if (backend == "couchdb") {
        kvConfig = std::make_unique<CouchKVStoreConfig>(
                config, backend, numShards, shardId);
    } else if (backend == "nexus") {
        kvConfig = std::make_unique<NexusKVStoreConfig>(
                config, backend, numShards, shardId);
    }
#ifdef EP_USE_MAGMA
    else if (backend == "magma") {
        kvConfig = std::make_unique<MagmaKVStoreConfig>(
                config, backend, numShards, shardId);
    }
#endif
    else {
        throw std::logic_error(
                "KVStoreConfig::createKVStoreConfig: Invalid backend type '" +
                std::string(backend) + "'");
    }
    return kvConfig;
}
