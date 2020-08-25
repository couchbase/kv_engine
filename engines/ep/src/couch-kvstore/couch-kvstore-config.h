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

#pragma once

#include "kvstore_config.h"

class CouchKVStoreConfig : public KVStoreConfig {
public:
    /**
     * This constructor intialises the object from a central
     * ep-engine Configuration instance.
     */
    CouchKVStoreConfig(Configuration& config,
                       uint16_t maxShards,
                       uint16_t shardId);

    /**
     * This constructor sets the mandatory config options
     *
     * Optional config options are set using a separate method
     */
    CouchKVStoreConfig(uint16_t maxVBuckets,
                       uint16_t maxShards,
                       const std::string& dbname,
                       const std::string& backend,
                       uint16_t shardId);

    /**
     * Used to override the default buffering behaviour.
     *
     * Only recognised by CouchKVStore
     */
    void setBuffered(bool value) {
        buffered = value;
    }

    /**
     * Indicates whether or not underlying file operations will be
     * buffered by the storage engine used.
     *
     * Only recognised by CouchKVStore
     */
    bool getBuffered() const {
        return buffered;
    }

    void setCouchstoreTracingEnabled(bool value) {
        couchstoreTracingEnabled = value;
    }

    bool getCouchstoreTracingEnabled() const {
        return couchstoreTracingEnabled;
    }

    void setCouchstoreWriteValidationEnabled(bool value) {
        couchstoreWriteValidationEnabled = value;
    }

    bool getCouchstoreWriteValidationEnabled() const {
        return couchstoreWriteValidationEnabled;
    }

    void setCouchstoreMprotectEnabled(bool value) {
        couchstoreMprotectEnabled = value;
    }

    bool getCouchstoreMprotectEnabled() const {
        return couchstoreMprotectEnabled;
    }

    void setCouchstoreFileCacheMaxSize(size_t value);

private:
    class ConfigChangeListener;

    bool buffered;

    // Following config variables are atomic as can be changed (via
    // ConfigChangeListener) at runtime by front-end threads while read by
    // IO threads.

    /* enable tracing for couchstore */
    std::atomic_bool couchstoreTracingEnabled;
    /* enable write verification for couchstore */
    std::atomic_bool couchstoreWriteValidationEnabled;
    /* enbale mprotect of couchstore internal io buffer */
    std::atomic_bool couchstoreMprotectEnabled;
};