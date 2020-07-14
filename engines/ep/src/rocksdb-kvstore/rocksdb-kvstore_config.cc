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

#include "rocksdb-kvstore_config.h"

#include "configuration.h"

RocksDBKVStoreConfig::RocksDBKVStoreConfig(Configuration& config,
                                           uint16_t numShards,
                                           uint16_t shardid)
    : KVStoreConfig(config, numShards, shardid) {
    // The RocksDB Options, CFOptions and BBTOptions in 'configuration.json'
    // are comma-separated <option>=<value> pairs, but RocksDB can parse only
    // semicolon-separated option strings. We cannot use directly the semicolon
    // because the `-e "<config>"` command-line param in tests already uses it
    // as separator in the '<config>' string.
    dbOptions = config.getRocksdbOptions();
    cfOptions = config.getRocksdbCfOptions();
    bbtOptions = config.getRocksdbBbtOptions();
    std::replace(dbOptions.begin(), dbOptions.end(), ',', ';');
    std::replace(cfOptions.begin(), cfOptions.end(), ',', ';');
    std::replace(bbtOptions.begin(), bbtOptions.end(), ',', ';');

    bucketQuota = config.getMaxSize();
    lowPriBackgroundThreads = config.getRocksdbLowPriBackgroundThreads();
    highPriBackgroundThreads = config.getRocksdbHighPriBackgroundThreads();
    statsLevel = config.getRocksdbStatsLevel();
    blockCacheRatio = config.getRocksdbBlockCacheRatio();
    blockCacheHighPriPoolRatio = config.getRocksdbBlockCacheHighPriPoolRatio();
    memtablesRatio = config.getRocksdbMemtablesRatio();
    defaultCfOptimizeCompaction =
            config.getRocksdbDefaultCfOptimizeCompaction();
    seqnoCfOptimizeCompaction = config.getRocksdbSeqnoCfOptimizeCompaction();
    writeRateLimit = config.getRocksdbWriteRateLimit();
    ucMaxSizeAmplificationPercent =
            config.getRocksdbUcMaxSizeAmplificationPercent();
}

std::shared_ptr<rocksdb::RateLimiter>
RocksDBKVStoreConfig::getEnvRateLimiter() {
    if (writeRateLimit > 0) {
        // IO rate limiter for background Flushers and Compactors.
        // 'rateLimiter' is shared across all the RocksDB instances in the
        // current environment (i.e., across all buckets on the node) to enforce
        // a global rate limit.
        static std::shared_ptr<rocksdb::RateLimiter> rateLimiter(
                rocksdb::NewGenericRateLimiter(writeRateLimit));
        return rateLimiter;
    }
    return nullptr;
}
