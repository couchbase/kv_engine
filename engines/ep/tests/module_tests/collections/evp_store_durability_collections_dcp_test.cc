/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2020 Couchbase, Inc
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

/**
 * Tests for Collection functionality in EPStore.
 */
#include "tests/module_tests/collections/evp_store_durability_collections_dcp_test.h"
#include "dcp/response.h"
#include "kv_bucket.h"
#include "tests/mock/mock_stream.h"
#include "tests/module_tests/collections/test_manifest.h"

#include <engines/ep/tests/ep_test_apis.h>

TEST_P(CollectionsDurabilityDcpParameterizedTest,
       seqno_advanced_one_mutation_plus_pending) {
    VBucketPtr vb = store->getVBucket(vbid);
    CollectionsManifest cm{};
    store->setCollections(std::string{
            cm.add(CollectionEntry::meat).add(CollectionEntry::dairy)});
    // filter only CollectionEntry::dairy
    createDcpObjects({{R"({"collections":["c"]})"}});

    store_item(vbid, StoredDocKey{"meat::one", CollectionEntry::meat}, "pork");
    store_item(vbid, StoredDocKey{"meat::two", CollectionEntry::meat}, "beef");
    store_item(vbid,
               StoredDocKey{"dairy::three", CollectionEntry::dairy},
               "cheese");
    store_item(
            vbid,
            StoredDocKey{"dairy::four", CollectionEntry::dairy},
            std::string("milk"),
            0,
            {cb::engine_errc::sync_write_pending},
            PROTOCOL_BINARY_RAW_BYTES,
            cb::durability::Requirements(cb::durability::Level::Majority,
                                         cb::durability::Timeout::Infinity()));

    // 2 collections + 3 mutations
    flushVBucketToDiskIfPersistent(vbid, 6);

    notifyAndStepToCheckpoint(cb::mcbp::ClientOpcode::DcpSnapshotMarker);
    stepAndExpect(cb::mcbp::ClientOpcode::DcpSystemEvent,
                  cb::engine_errc::success);
    EXPECT_EQ(producers->last_collection_id, CollectionEntry::dairy.getId());
    stepAndExpect(cb::mcbp::ClientOpcode::DcpMutation,
                  cb::engine_errc::success);
    EXPECT_EQ(producers->last_collection_id, CollectionEntry::dairy.getId());
    stepAndExpect(cb::mcbp::ClientOpcode::DcpSeqnoAdvanced,
                  cb::engine_errc::success);
}

// Test cases which run for persistent and ephemeral buckets
INSTANTIATE_TEST_SUITE_P(CollectionsDurabilityDcpEphemeralOrPersistent,
                         CollectionsDurabilityDcpParameterizedTest,
                         STParameterizedBucketTest::allConfigValues(),
                         STParameterizedBucketTest::PrintToStringParamName);
