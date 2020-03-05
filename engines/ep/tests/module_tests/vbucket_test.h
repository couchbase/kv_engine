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
#include "stats.h"
#include "vbucket.h"
#include "vbucket_queue_item_ctx.h"

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

struct SWCompleteTrace {
    SWCompleteTrace() {
    }
    SWCompleteTrace(uint8_t count, const void* cookie, ENGINE_ERROR_CODE status)
        : count(count), cookie(cookie), status(status) {
    }
    uint8_t count{0};
    const void* cookie{nullptr};
    ENGINE_ERROR_CODE status{ENGINE_EINVAL}; // just a placeholder

    friend bool operator==(const SWCompleteTrace& lhs,
                           const SWCompleteTrace& rhs);
};

bool operator==(const SWCompleteTrace& lhs, const SWCompleteTrace& rhs);

/**
 * Base class for VBucket tests, for example DefragmenterTest
 */
class VBucketTestBase {
public:
    enum class VBType { Persistent, Ephemeral };

    static std::string to_string(VBType vbtype);

    /**
     * Construct test objects with the given vBucket type and eviction policy
     */
    VBucketTestBase(VBType vbType, EvictionPolicy policy);
    ~VBucketTestBase();

protected:
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
            const StoredDocKey& key, const VBQueueItemCtx& ctx = {});

    MutationStatus public_processSet(Item& itm,
                                     const uint64_t cas,
                                     const VBQueueItemCtx& ctx = {});

    AddStatus public_processAdd(Item& itm, const VBQueueItemCtx& ctx = {});

    /// Public access to processSoftDelete() method.
    std::pair<MutationStatus, StoredValue*> public_processSoftDelete(
            const DocKey& key, VBQueueItemCtx ctx = {});
    std::pair<MutationStatus, StoredValue*> public_processSoftDelete(
            HashTable::FindUpdateResult& htRes,
            StoredValue& v,
            VBQueueItemCtx ctx = {});

    bool public_deleteStoredValue(const DocKey& key);

    std::pair<MutationStatus, GetValue> public_getAndUpdateTtl(
            const DocKey& key, time_t exptime);

    SWCompleteTrace swCompleteTrace;

    // Mock SyncWriteCompleteCallback that helps in testing client-notify for
    // Commit/Abort
    const SyncWriteCompleteCallback TracedSyncWriteCompleteCb =
            [this](const void* cookie, ENGINE_ERROR_CODE status) {
                swCompleteTrace.count++;
                swCompleteTrace.cookie = cookie;
                swCompleteTrace.status = status;
            };

    /**
     * A SyncWriteResolvedCallback which does nothing (as we don't have a
     * background task to wake up which is the non-unit-test configuration.
     * Test(s) must manually invoke VBucket::processResolvedSyncWrites()
     * after the relevant operation.
     */
    const SyncWriteResolvedCallback noOpSyncWriteResolvedCb = [](Vbid) {
        return;
    };

    const Vbid vbid{0};
    std::unique_ptr<VBucket> vbucket;
    EPStats global_stats;
    CheckpointConfig checkpoint_config;
    Configuration config;
    const void* cookie = {};
    const uint64_t lastSeqno{1000};
    snapshot_range_t range{5, lastSeqno};
};

/**
 * Test fixture for VBucket-level tests which are applicable to both Ephemeral
 * and EP VBuckets.
 *
 * Paramterised on:
 * - The type of VBucket (ephemeral/persistent)
 * - The eviction policy
 */
class VBucketTest
    : public ::testing::TestWithParam<
              std::tuple<VBucketTestBase::VBType, EvictionPolicy>>,
      public VBucketTestBase {
public:
    VBucketTest()
        : VBucketTestBase(std::get<0>(GetParam()), std::get<1>(GetParam())) {
    }

    EvictionPolicy getEvictionPolicy() const {
        return std::get<1>(GetParam());
    }

    VBType getVbType() const {
        return std::get<0>(GetParam());
    }

    bool persistent() const {
        return getVbType() == VBType::Persistent;
    }

    static std::string PrintToStringParamName(
            const ::testing::TestParamInfo<ParamType>&);
};
