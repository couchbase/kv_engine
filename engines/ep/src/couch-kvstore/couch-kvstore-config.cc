/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2020 Couchbase, Inc
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

#include "couch-kvstore-config.h"

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
