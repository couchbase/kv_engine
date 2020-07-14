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

#pragma once

#include <atomic>
#include <chrono>
#include <string>

class BucketLogger;
class Configuration;

class KVStoreConfig {
public:
    /**
     * This constructor intialises the object from a central
     * ep-engine Configuration instance.
     */
    KVStoreConfig(Configuration& config, uint16_t numShards, uint16_t shardId);

    /**
     * This constructor sets the mandatory config options
     *
     * Optional config options are set using a separate method
     */
    KVStoreConfig(uint16_t _maxVBuckets,
                  uint16_t _maxShards,
                  std::string _dbname,
                  std::string _backend,
                  uint16_t _shardId);

    KVStoreConfig(const KVStoreConfig& other);

    virtual ~KVStoreConfig();

    uint16_t getMaxVBuckets() const {
        return maxVBuckets;
    }

    uint16_t getMaxShards() const {
        return maxShards;
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

    BucketLogger& getLogger() {
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

    std::chrono::seconds getPitrGranularity() const {
        return pitrGranularity;
    }

    void setMetadataPurgeAge(std::chrono::seconds age) {
        metadataPurgeAge = age;
    }

    std::chrono::seconds getMetadataPurgeAge() const {
        return metadataPurgeAge;
    }

protected:
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
    std::atomic<std::chrono::seconds> pitrGranularity;

    /**
     * Length of time for which we keep tombstones before purging to allow
     * DCP clients to reconnect without rollback
     */
    std::atomic<std::chrono::seconds> metadataPurgeAge;
};
