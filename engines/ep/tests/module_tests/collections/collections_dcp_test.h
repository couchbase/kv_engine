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

#include "collections/kvstore.h"
#include "tests/mock/mock_dcp.h"
#include "tests/module_tests/collections/collections_dcp_producers.h"
#include "tests/module_tests/evp_store_single_threaded_test.h"
#include <memcached/dcp_stream_id.h>
#include <utilities/test_manifest.h>

class MockDcpConsumer;
class MockDcpProducer;

class CollectionsDcpTest : virtual public SingleThreadedKVBucketTest {
public:
    CollectionsDcpTest();

    // Setup a producer/consumer ready for the test
    void SetUp() override;

    void internalSetUp();

    Collections::KVStore::Manifest getPersistedManifest(Vbid vb) const;

    void createDcpStream(
            std::optional<std::string_view> collections,
            Vbid id = Vbid(0),
            cb::engine_errc expectedError = cb::engine_errc::success,
            uint32_t flags = 0);

    void createDcpConsumer();

    void createDcpObjects(std::optional<std::string_view> collections,
                          bool enableOutOfOrderSnapshots = false,
                          uint32_t flags = 0,
                          bool enableSyncRep = false);

    void TearDown() override;

    void teardown();

    void runCheckpointProcessor();

    void notifyAndStepToCheckpoint(
            cb::mcbp::ClientOpcode expectedOp =
                    cb::mcbp::ClientOpcode::DcpSnapshotMarker,
            bool fromMemory = true);

    void stepAndExpect(cb::mcbp::ClientOpcode opcode,
                       cb::engine_errc err = cb::engine_errc::success);

    /// the vectors are ordered, so front() is the first item we expect to see
    void testDcpCreateDelete(
            const std::vector<CollectionEntry::Entry>& expectedCreates,
            const std::vector<CollectionEntry::Entry>& expectedDeletes,
            int expectedMutations,
            bool fromMemory = true,
            const std::vector<ScopeEntry::Entry>& expectedScopeCreates = {},
            const std::vector<ScopeEntry::Entry>& expectedScopeDrops = {},
            bool compareManifests = true);

    void resetEngineAndWarmup(std::string new_config = "");

    /**
     * This function will
     * 1) clear the checkpoint on the active source vbucket (vbid)
     * 2) change the value of the replica vbid so that it is safe to replay
     *    vbid against replicaVbid, which will now be a 'blank' canvas
     *
     * After this call a test should be able to guarantee the next DCP stream
     * will be backfilled from disk/seqlist
     */
    void ensureDcpWillBackfill();

    void tombstone_snapshots_test(bool forceWarmup);

    void runEraser();

    static ENGINE_ERROR_CODE dcpAddFailoverLog(
            const std::vector<vbucket_failover_t>&);

    cb::tracing::Traceable* cookieC;
    cb::tracing::Traceable* cookieP;
    std::unique_ptr<CollectionsDcpTestProducers> producers;
    std::shared_ptr<MockDcpProducer> producer;
    std::shared_ptr<MockDcpConsumer> consumer;
    Vbid replicaVB;
};

/**
 * Test for Collection functionality in both EventuallyPersistent and
 * Ephemeral buckets.
 */
class CollectionsDcpParameterizedTest : public STParameterizedBucketTest,
                                        public CollectionsDcpTest {
public:
    void SetUp() override {
        STParameterizedBucketTest::SetUp();
        CollectionsDcpTest::internalSetUp();
    }

    void TearDown() override {
        CollectionsDcpTest::TearDown();
    }

protected:
    /**
     * Test what happens to the manifest when we promote a vBucket
     */
    void testVBPromotionUpdateManifest();

    /**
     * Test forced updates with replication
     */
    void replicateForcedUpdate(uint64_t newUid, bool warmup);
};
