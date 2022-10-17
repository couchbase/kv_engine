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

#pragma once

#include "ep_bucket.h"

#include <folly/portability/GMock.h>

class ItemFreqDecayerTask;
class MockItemFreqDecayerTask;

/*
 * Mock of the EPBucket class.
 */
class MockEPBucket : public EPBucket {
public:
    explicit MockEPBucket(EventuallyPersistentEngine& theEngine);

    MOCK_METHOD(void,
                dropKey,
                (VBucket&, const DiskDocKey&, int64_t, bool, int64_t),
                (override));

    using KVBucket::initializeExpiryPager;

    /**
     * Mock specific initialization. Does not override initialize function as
     * the general use of this mock requires avoiding the initialization of
     * background tasks
     */
    void initializeMockBucket();

    void createItemFreqDecayerTask();

    void disableItemFreqDecayerTask();

    ItemFreqDecayerTask* getItemFreqDecayerTask();
    MockItemFreqDecayerTask* getMockItemFreqDecayerTask();

    VBucketPtr makeVBucket(Vbid id,
                           vbucket_state_t state,
                           KVShard* shard,
                           std::unique_ptr<FailoverTable> table,
                           NewSeqnoCallback newSeqnoCb,
                           std::unique_ptr<Collections::VB::Manifest> manifest,
                           vbucket_state_t initState,
                           int64_t lastSeqno,
                           uint64_t lastSnapStart,
                           uint64_t lastSnapEnd,
                           uint64_t purgeSeqno,
                           uint64_t maxCas,
                           int64_t hlcEpochSeqno,
                           bool mightContainXattrs,
                           const nlohmann::json* replicationTopology,
                           uint64_t maxVisibleSeqno,
                           uint64_t maxPrepareSeqno) override;

    void setDurabilityCompletionTask(
            std::shared_ptr<DurabilityCompletionTask> task);

    /// @returns a non-const pointer to Flusher object.
    Flusher* getFlusherNonConst(Vbid vbid);

    void setPostCompactionCompletionHook(std::function<void()> hook) {
        postCompactionCompletionStatsUpdateHook = hook;
    }

    void removeMakeCompactionContextCallback();

    void completeBGFetchMulti(
            Vbid vbId,
            std::vector<bgfetched_item_t>& fetchedItems,
            std::chrono::steady_clock::time_point start) override;

    void publicCompactionCompletionCallback(CompactionContext& ctx);

    TestingHook<Vbid> completeBGFetchMultiHook;

    std::shared_ptr<CompactTask> getCompactionTask(Vbid vbid) const;
    std::shared_ptr<CompactionContext> makeCompactionContext(
            Vbid vbid, CompactionConfig& config, uint64_t purgeSeqno) override;
    std::function<std::shared_ptr<CompactionContext>(
            std::shared_ptr<CompactionContext>)>
            mockMakeCompactionContext =
                    [](std::shared_ptr<CompactionContext> ctx) { return ctx; };

    cb::AwaitableSemaphore& public_getCompactionSemaphore();

    void public_updateCompactionConcurrency();
};
