/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2016 Couchbase, Inc
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

#include "callbacks.h"
#include "checkpoint_config.h"
#include "configuration.h"
#include "hash_table.h"
#include "item_pager.h"
#include "stats.h"
#include "vbucket.h"

#include <folly/portability/GTest.h>

class BgFetcher;
class VBucket;
class VBucketBGFetchItem;

/**
 * Dummy callback to replace the flusher callback.
 */
class DummyCB : public Callback<Vbid> {
public:
    DummyCB() {}

    void callback(Vbid& dummy) {
    }
};

/**
 * Test fixture for VBucket tests.
 *
 * Templated on the Item Eviction policy to use.
 */
class VBucketTest
    : virtual public ::testing::Test,
      public ::testing::WithParamInterface<item_eviction_policy_t> {
protected:
    void SetUp();

    void TearDown();

    std::vector<StoredDocKey> generateKeys(int num, int start = 0);

    // Create a queued item with the given key
    queued_item makeQueuedItem(const char *key);

    AddStatus addOne(const StoredDocKey& k, int expiry = 0);

    TempAddStatus addOneTemp(const StoredDocKey& k);

    void addMany(std::vector<StoredDocKey>& keys, AddStatus expect);

    MutationStatus setOne(const StoredDocKey& k, int expiry = 0);

    void setMany(std::vector<StoredDocKey>& keys, MutationStatus expect);

    void softDeleteOne(const StoredDocKey& k, MutationStatus expect);

    void softDeleteMany(std::vector<StoredDocKey>& keys, MutationStatus expect);

    StoredValue* findValue(StoredDocKey& key);

    void verifyValue(StoredDocKey& key,
                     const char* value,
                     TrackReference trackReference,
                     WantsDeleted wantDeleted);

    std::pair<HashTable::HashBucketLock, StoredValue*> lockAndFind(
            const StoredDocKey& key);

    MutationStatus public_processSet(Item& itm,
                                     const uint64_t cas,
                                     const VBQueueItemCtx& ctx = {});

    AddStatus public_processAdd(Item& itm);

    MutationStatus public_processSoftDelete(const DocKey& key,
                                            StoredValue* v,
                                            uint64_t cas);

    bool public_deleteStoredValue(const DocKey& key);

    GetValue public_getAndUpdateTtl(const DocKey& key, time_t exptime);

    void public_incrementBackfillQueueSize();

    struct {
        uint8_t count{0};
        const void* cookie{nullptr};
        ENGINE_ERROR_CODE status{ENGINE_EINVAL}; // just a placeholder
    } swCompleteTrace;

    // Mock SyncWriteCompleteCallback that helps in testing client-notify for
    // Commit/Abort
    const SyncWriteCompleteCallback TracedSyncWriteCompleteCb =
            [this](const void* cookie, ENGINE_ERROR_CODE status) {
                swCompleteTrace.count++;
                swCompleteTrace.cookie = cookie;
                swCompleteTrace.status = status;
            };

    std::unique_ptr<VBucket> vbucket;
    EPStats global_stats;
    CheckpointConfig checkpoint_config;
    Configuration config;
    const void* cookie;
};
