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

class Configuration;

// This class represents the MagmaKVStore specific configuration.
// MagmaKVStore uses this in place of the KVStoreConfig base class.
class MagmaKVStoreConfig : public KVStoreConfig {
public:
    // Initialize the object from the central EPEngine Configuration
    MagmaKVStoreConfig(Configuration& config, uint16_t shardid);

    size_t getBucketQuota() {
        return bucketQuota;
    }
    int getMagmaMaxCommitPoints() const {
        return magmaMaxCommitPoints;
    }
    size_t getMagmaMaxWriteCache() const {
        return magmaMaxWriteCache;
    }
    float getMagmaMemQuotaRatio() const {
        return magmaMemQuotaRatio;
    }
    size_t getMagmaMinValueSize() const {
        return magmaMinValueSize;
    }
    int getMagmaNumFlushers() const {
        return magmaNumFlushers;
    }
    int getMagmaNumCompactors() const {
        return magmaNumCompactors;
    }
    size_t getMagmaWalBufferSize() const {
        return magmaWalBufferSize;
    }

private:
    // Bucket RAM Quota
    size_t bucketQuota;

    // Max commit points that can be rolled back to
    int magmaMaxCommitPoints;

    // Magma maximum write cache max size
    size_t magmaMaxWriteCache;

    // Magma Memory Quota as a ratio of Bucket Quota
    float magmaMemQuotaRatio;

    // Magma minimum value for key value separation
    size_t magmaMinValueSize;

    // Number of background threads to flush filled memtables to disk
    int magmaNumFlushers;

    // Number of background compactor threads
    int magmaNumCompactors;

    // Magma WAL Buffer Size after which flush will be forced
    size_t magmaWalBufferSize;
};
