/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2019 Couchbase, Inc.
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

#include "evp_vbucket_test.h"

#include "bgfetcher.h"
#include "ep_vb.h"
#include "tests/mock/mock_synchronous_ep_engine.h"
#include "tests/module_tests/test_helpers.h"

void EPVBucketTest::SetUp() {
    SingleThreadedKVBucketTest::SetUp();
}

void EPVBucketTest::TearDown() {
    SingleThreadedKVBucketTest::TearDown();
}

size_t EPVBucketTest::public_queueBGFetchItem(
        const DocKey& key,
        std::unique_ptr<VBucketBGFetchItem> fetchItem,
        BgFetcher* bgFetcher) {
    return dynamic_cast<EPVBucket&>(*vbucket).queueBGFetchItem(
            key, std::move(fetchItem), bgFetcher);
}

/**
 * To test the BgFetcher with a VBucket we need references to an EPBucket and
 * a KVShard. The easiest way to create these is to create an entire engine...
 *
 * Measure performance of VBucket::getBGFetchItems - queue and then get
 * 100,000 items from the vbucket.
 */
TEST_P(EPVBucketTest, GetBGFetchItemsPerformance) {
    // Use the SynchronousEPEngine to create the necessary objects for our
    // bgFetcher
    auto mockEPBucket =
            engine->public_makeMockBucket(engine->getConfiguration());
    KVShard kvShard(0, engine->getConfiguration());
    BgFetcher bgFetcher(*mockEPBucket.get(), kvShard);

    for (unsigned int ii = 0; ii < 100000; ii++) {
        auto fetchItem = std::make_unique<VBucketBGFetchItem>(nullptr,
                                                              /*isMeta*/ false);
        this->public_queueBGFetchItem(makeStoredDocKey(std::to_string(ii)),
                                      std::move(fetchItem),
                                      &bgFetcher);
    }
    auto items = this->vbucket->getBGFetchItems();
}

INSTANTIATE_TEST_CASE_P(
        FullAndValueEviction,
        EPVBucketTest,
        ::testing::Values(VALUE_ONLY, FULL_EVICTION),
        [](const ::testing::TestParamInfo<item_eviction_policy_t>& info) {
            if (info.param == VALUE_ONLY) {
                return "VALUE_ONLY";
            } else {
                return "FULL_EVICTION";
            }
        });
