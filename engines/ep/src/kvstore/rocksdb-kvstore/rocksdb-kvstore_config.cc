/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2018-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "rocksdb-kvstore_config.h"

#include "configuration.h"

RocksDBKVStoreConfig::RocksDBKVStoreConfig(Configuration& config,
                                           std::string_view backend,
                                           uint16_t numShards,
                                           uint16_t shardid)
    : KVStoreConfig(config, backend, numShards, shardid) {
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
