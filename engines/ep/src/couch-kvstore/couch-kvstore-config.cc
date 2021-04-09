/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2020-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "couch-kvstore-config.h"

#include "configuration.h"
#include "couch-kvstore-file-cache.h"

/// A listener class to update KVStore related configs at runtime.
class CouchKVStoreConfig::ConfigChangeListener : public ValueChangedListener {
public:
    explicit ConfigChangeListener(CouchKVStoreConfig& c) : config(c) {
    }

    void booleanValueChanged(const std::string& key, bool value) override {
        if (key == "couchstore_tracing") {
            config.setCouchstoreTracingEnabled(value);
        }
        if (key == "couchstore_write_validation") {
            config.setCouchstoreWriteValidationEnabled(value);
        }
        if (key == "couchstore_mprotect") {
            config.setCouchstoreMprotectEnabled(value);
        }
    }

    void sizeValueChanged(const std::string& key, size_t value) override {
        if (key == "couchstore_file_cache_max_size") {
            config.setCouchstoreFileCacheMaxSize(value);
        }
    }

private:
    CouchKVStoreConfig& config;
};

CouchKVStoreConfig::CouchKVStoreConfig(Configuration& config,
                                       uint16_t maxShards,
                                       uint16_t shardId)
    : KVStoreConfig(config, maxShards, shardId), buffered(true) {
    setCouchstoreTracingEnabled(config.isCouchstoreTracing());
    config.addValueChangedListener(
            "couchstore_tracing",
            std::make_unique<ConfigChangeListener>(*this));
    setCouchstoreWriteValidationEnabled(config.isCouchstoreWriteValidation());
    config.addValueChangedListener(
            "couchstore_write_validation",
            std::make_unique<ConfigChangeListener>(*this));
    setCouchstoreMprotectEnabled(config.isCouchstoreMprotect());
    config.addValueChangedListener(
            "couchstore_mprotect",
            std::make_unique<ConfigChangeListener>(*this));
    setCouchstoreFileCacheMaxSize(config.getCouchstoreFileCacheMaxSize());
    config.addValueChangedListener(
            "couchstore_file_cache_max_size",
            std::make_unique<ConfigChangeListener>(*this));
}

CouchKVStoreConfig::CouchKVStoreConfig(uint16_t maxVBuckets,
                                       uint16_t maxShards,
                                       const std::string& dbname,
                                       const std::string& backend,
                                       uint16_t shardId)
    : KVStoreConfig(maxVBuckets, maxShards, dbname, backend, shardId),
      buffered(true),
      couchstoreTracingEnabled(false),
      couchstoreWriteValidationEnabled(false),
      couchstoreMprotectEnabled(false) {
}

void CouchKVStoreConfig::setCouchstoreFileCacheMaxSize(size_t value) {
    CouchKVStoreFileCache::get().getHandle()->resize(value);
}
