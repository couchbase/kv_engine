/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2016 Couchbase, Inc.
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

#include "kv_bucket.h"

/**
 * Eventually Persistent Bucket
 *
 * A bucket type which stores modifications to disk asynchronously
 * ("eventually").
 * Uses hash partitioning of the keyspace into VBuckets, to support
 * replication, rebalance, failover.
 */

class EPBucket : public KVBucket {
public:
    EPBucket(EventuallyPersistentEngine& theEngine);

    bool initialize() override;

    void deinitialize() override;

    protocol_binary_response_status evictKey(const DocKey& key,
                                             uint16_t vbucket,
                                             const char **msg,
                                             size_t *msg_size) override;

    void reset() override;

    /// Start the Flusher for all shards in this bucket.
    void startFlusher();

    /// Stop the Flusher for all shards in this bucket.
    void stopFlusher();

    bool pauseFlusher() override;
    bool resumeFlusher() override;

    void wakeUpFlusher() override;

    /**
     * Starts the background fetcher for each shard.
     * @return true if successful.
     */
    bool startBgFetcher();

    /// Stops the background fetcher for each shard.
    void stopBgFetcher();

    ENGINE_ERROR_CODE getFileStats(const void* cookie,
                                   ADD_STAT add_stat) override;

    ENGINE_ERROR_CODE getPerVBucketDiskStats(const void* cookie,
                                             ADD_STAT add_stat) override;
    /**
     * Creates a VBucket object.
     */
    RCPtr<VBucket> makeVBucket(VBucket::id_type id,
                               vbucket_state_t state,
                               KVShard* shard,
                               std::unique_ptr<FailoverTable> table,
                               NewSeqnoCallback newSeqnoCb,
                               vbucket_state_t initState,
                               int64_t lastSeqno,
                               uint64_t lastSnapStart,
                               uint64_t lastSnapEnd,
                               uint64_t purgeSeqno,
                               uint64_t maxCas) override;
};
