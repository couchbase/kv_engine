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

#include "config.h"

#include "callbacks.h"
#include "checkpoint.h"
#include "item_pager.h"
#include "hash_table.h"

#include <gtest/gtest.h>

class VBucket;

/**
 * Dummy callback to replace the flusher callback.
 */
class DummyCB: public Callback<uint16_t> {
public:
    DummyCB() {}

    void callback(uint16_t &dummy) { }
};

/**
 * Test fixture for VBucket tests.
 *
 * Templated on the Item Eviction policy to use.
 */
class VBucketTest
        : public ::testing::Test,
          public ::testing::WithParamInterface<item_eviction_policy_t> {
protected:
    void SetUp();

    void TearDown();

    std::vector<StoredDocKey> generateKeys(int num, int start = 0);

    void addOne(const StoredDocKey& k, AddStatus expect, int expiry = 0);

    void addMany(std::vector<StoredDocKey>& keys, AddStatus expect);

    void setOne(const StoredDocKey& k, MutationStatus expect, int expiry = 0);

    void setMany(std::vector<StoredDocKey>& keys, MutationStatus expect);

    void softDeleteOne(const StoredDocKey& k, MutationStatus expect);

    void softDeleteMany(std::vector<StoredDocKey>& keys, MutationStatus expect);

    void verifyValue(StoredDocKey& key,
                     const char* value,
                     TrackReference trackReference,
                     WantsDeleted wantDeleted);

    std::pair<HashTable::HashBucketLock, StoredValue*> lockAndFind(
            const StoredDocKey& key);

    MutationStatus public_processSet(Item& itm, const uint64_t cas);

    AddStatus public_processAdd(Item& itm);

    MutationStatus public_processSoftDelete(const DocKey& key,
                                            StoredValue* v,
                                            uint64_t cas);

    bool public_deleteStoredValue(const DocKey& key);

    std::unique_ptr<VBucket> vbucket;
    EPStats global_stats;
    CheckpointConfig checkpoint_config;
    Configuration config;
};

class EPVBucketTest : public VBucketTest {
protected:
    size_t public_queueBGFetchItem(
            const DocKey& key,
            std::unique_ptr<VBucketBGFetchItem> fetchItem,
            BgFetcher* bgFetcher);
};
