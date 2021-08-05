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

#pragma once

#include <atomic>
#include <chrono>
#include <memory>
#include <string>

class BucketLogger;
class Configuration;

class KVStoreConfig {
public:
    /**
     * Generate a KVStoreConfig of the appropriate type using the given
     * parameters.
     *
     * @param config Bucket configuration
     * @param backend The type of KVStoreConfig/KVStore to be created
     * @param numShards Number of configured shard
     * @param shardId ID of the shard that this KVStoreConfig will belong to
     * @return KVStoreConfig
     */
    static std::unique_ptr<KVStoreConfig> createKVStoreConfig(
            Configuration& config,
            std::string_view backend,
            uint16_t numShards,
            uint16_t shardId);

    KVStoreConfig(const KVStoreConfig& other);

    virtual ~KVStoreConfig();

    uint16_t getMaxVBuckets() const {
        return maxVBuckets;
    }

    uint16_t getMaxShards() const {
        return maxShards;
    }

    void setDBName(std::string dbName) {
        dbname = dbName;
    }

    std::string getDBName() const {
        return dbname;
    }

    const std::string& getBackend() const {
        return backend;
    }

    uint16_t getShardId() const {
        return shardId;
    }

    BucketLogger& getLogger() const {
        return *logger;
    }

    /**
     * Used to override the default logger object
     */
    KVStoreConfig& setLogger(BucketLogger& _logger);

    uint64_t getPeriodicSyncBytes() const {
        return periodicSyncBytes;
    }

    void setPeriodicSyncBytes(uint64_t bytes) {
        periodicSyncBytes = bytes;
    }

    size_t getMaxFileDescriptors() const {
        return maxFileDescriptors;
    }

    bool isPitrEnabled() const {
        return pitrEnabled;
    }

    void setPitrEnabled(bool enabled) {
        pitrEnabled = enabled;
    }

    std::chrono::seconds getPitrMaxHistoryAge() const {
        return pitrMaxHistoryAge;
    }

    void setPitrMaxHistoryAge(std::chrono::seconds age) {
        pitrMaxHistoryAge = age;
    }

    void setPitrGranularity(std::chrono::seconds granularity) {
        pitrGranularity = granularity;
    }

    void setPitrGranularity(std::chrono::nanoseconds granularity) {
        pitrGranularity = granularity;
    }

    std::chrono::nanoseconds getPitrGranularity() const {
        return pitrGranularity;
    }

    void setMetadataPurgeAge(std::chrono::seconds age) {
        metadataPurgeAge = age;
    }

    std::chrono::seconds getMetadataPurgeAge() const {
        return metadataPurgeAge;
    }

    /// @returns the size to use for the cached values in the KVStores
    size_t getCacheSize() const;

protected:
    /**
     * This constructor intialises the object from a central
     * ep-engine Configuration instance.
     */
    KVStoreConfig(Configuration& config,
                  std::string_view backend,
                  uint16_t numShards,
                  uint16_t shardId);

    /**
     * This constructor sets the mandatory config options
     *
     * Optional config options are set using a separate method
     */
    KVStoreConfig(uint16_t _maxVBuckets,
                  uint16_t _maxShards,
                  std::string _dbname,
                  std::string_view _backend,
                  uint16_t _shardId);

    class ConfigChangeListener;

    uint16_t maxVBuckets;
    uint16_t maxShards;
    std::string dbname;
    std::string backend;
    uint16_t shardId;
    BucketLogger* logger;

    /**
     * Maximum number of file descriptors that the backend may use. This is the
     * maximum number across all shards and buckets.
     */
    size_t maxFileDescriptors;

    // Following config variables are atomic as can be changed (via
    // ConfigChangeListener) at runtime by front-end threads while read by
    // IO threads.

    /**
     * If non-zero, tell storage layer to issue a sync() operation after every
     * N bytes written.
     */
    std::atomic<uint64_t> periodicSyncBytes;

    std::atomic_bool pitrEnabled{false};
    std::atomic<std::chrono::seconds> pitrMaxHistoryAge;
    /// Granularity is stored in nanoseconds to allow for unit tests to execute
    /// faster
    std::atomic<std::chrono::nanoseconds> pitrGranularity;

    /**
     * Length of time for which we keep tombstones before purging to allow
     * DCP clients to reconnect without rollback
     */
    std::atomic<std::chrono::seconds> metadataPurgeAge;
};
