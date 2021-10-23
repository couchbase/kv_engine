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

    void createScopeOnConsumer(Vbid id,
                               uint32_t opaque,
                               Collections::ManifestUid muid,
                               const ScopeEntry::Entry& entry,
                               uint64_t seqno);

    void createCollectionOnConsumer(Vbid id,
                                    uint32_t opaque,
                                    Collections::ManifestUid muid,
                                    ScopeID sid,
                                    const CollectionEntry::Entry& entry,
                                    uint64_t seqno);

    static cb::engine_errc dcpAddFailoverLog(
            const std::vector<vbucket_failover_t>&);

    CookieIface* cookieC;
    CookieIface* cookieP;
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
