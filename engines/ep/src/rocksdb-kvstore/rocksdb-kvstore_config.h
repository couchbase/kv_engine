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

#pragma once

#include "kvstore_config.h"

#include <rocksdb/rate_limiter.h>

#include <string>

class Configuration;

// This class represents the RocksDBKVStore specific configuration.
// RocksDBKVStore uses this in place of the KVStoreConfig base class.
class RocksDBKVStoreConfig : public KVStoreConfig {
public:
    // Initialize the object from the central EPEngine Configuration
    RocksDBKVStoreConfig(Configuration& config, uint16_t shardid);

    // Return the Bucket Quota
    size_t getBucketQuota() {
        return bucketQuota;
    }

    // Return the Database level options.
    const std::string& getDBOptions() {
        return dbOptions;
    }

    // Return the Column Family level options.
    const std::string& getCFOptions() {
        return cfOptions;
    }

    // Return the Block Based Table options.
    const std::string& getBBTOptions() {
        return bbtOptions;
    }

    // Return the low priority background thread count.
    size_t getLowPriBackgroundThreads() const {
        return lowPriBackgroundThreads;
    }

    // Return the high priority background thread count.
    size_t getHighPriBackgroundThreads() const {
        return highPriBackgroundThreads;
    }

    // Return the Statistics 'stats_level'.
    const std::string& getStatsLevel() {
        return statsLevel;
    }

    // Return the Block Cache ratio of the Bucket Quota.
    float getBlockCacheRatio() {
        return blockCacheRatio;
    }

    // Ratio of the BlockCache quota reserved for index/filter blocks
    float getBlockCacheHighPriPoolRatio() {
        return blockCacheHighPriPoolRatio;
    }

    // Return the total Memtables ratio of the Bucket Quota
    float getMemtablesRatio() {
        return memtablesRatio;
    }

    // Return the Compaction Optimization type for the 'default' CF
    std::string getDefaultCfOptimizeCompaction() {
        return defaultCfOptimizeCompaction;
    }

    // Return the Compaction Optimization type for the 'seqno' CF
    std::string getSeqnoCfOptimizeCompaction() {
        return seqnoCfOptimizeCompaction;
    }

    // Return the write rate limit for Flush and Compaction
    size_t getWriteRateLimit() {
        return writeRateLimit;
    }

    // Creates a RateLimiter object, which is shared across all the RocksDB
    // instances in the environment to control the IO rate of Flush and
    // Compaction tasks.
    std::shared_ptr<rocksdb::RateLimiter> getEnvRateLimiter();

private:
    // Amount of memory reserved for the bucket
    size_t bucketQuota = 0;

    // Database level options. Semicolon-separated '<option>=<value>' pairs
    std::string dbOptions = "";

    // Column Family level options. Semicolon-separated '<option>=<value>' pairs
    std::string cfOptions = "";

    // Block Based Table options. Semicolon-separated '<option>=<value>' pairs
    std::string bbtOptions = "";

    // Low priority background thread count
    size_t lowPriBackgroundThreads = 0;

    // High priority background thread count
    size_t highPriBackgroundThreads = 0;

    // Statistics 'stats_level'. Possible values:
    // {'', 'kAll', 'kExceptTimeForMutex', 'kExceptDetailedTimers'}
    std::string statsLevel = "";

    // Block Cache ratio of the Bucket Quota
    float blockCacheRatio = 0.0;

    // Ratio of the BlockCache quota reserved for index/filter blocks
    float blockCacheHighPriPoolRatio = 0.0;

    // Total Memtables ratio of the Bucket Quota.
    // This ratio represents the total quota of memory allocated for the
    // Memtables of all Column Families. The logic in 'RocksDBKVStore' decides
    // how this quota is split among different CFs. If this ratio is set to
    // 0.0, then we set each Memtable size to a baseline value.
    float memtablesRatio = 0.0;

    // Flag to enable Compaction Optimization for the 'default' CF
    std::string defaultCfOptimizeCompaction = "";

    // Flag to enable Compaction Optimization for the 'seqno' CF
    std::string seqnoCfOptimizeCompaction = "";

    // Write rate limit. Use to control write rate of flush and
    // compaction.
    size_t writeRateLimit = 0;
};
