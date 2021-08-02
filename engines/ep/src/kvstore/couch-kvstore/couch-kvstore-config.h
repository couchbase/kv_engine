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

#pragma once

#include "kvstore/kvstore_config.h"

class CouchKVStoreConfig : public KVStoreConfig {
public:
    /**
     * This constructor intialises the object from a central
     * ep-engine Configuration instance.
     */
    CouchKVStoreConfig(Configuration& config,
                       std::string_view backend,
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