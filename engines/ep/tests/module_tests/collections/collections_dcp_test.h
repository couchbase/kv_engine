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

class DcpProducerConfig;
class DcpStreamRequestConfig;
class MockDcpConsumer;
class MockDcpProducer;

class CollectionsDcpTest : virtual public SingleThreadedKVBucketTest {
public:
    CollectionsDcpTest();

    // Setup a producer/consumer ready for the test
    void SetUp() override;

    void internalSetUp();

    /**
     * Moves the helperCursor to the end of the CheckpointList.
     */
    void moveHelperCursorToCMEnd();

    Collections::KVStore::Manifest getPersistedManifest(Vbid vb) const;

    void createDcpStream(
            std::optional<std::string_view> collections,
            Vbid id = Vbid(0),
            cb::engine_errc expectedError = cb::engine_errc::success,
            cb::mcbp::DcpAddStreamFlag flags = {},
            uint64_t streamEndSeqno = ~0ull);

    void createDcpStream(const DcpStreamRequestConfig& config);

    void createDcpConsumer(const DcpProducerConfig& producerConfig);

    void createDcpObjects(
            std::optional<std::string_view> collections,
            OutOfOrderSnapshots outOfOrderSnapshots = OutOfOrderSnapshots::No,
            cb::mcbp::DcpAddStreamFlag flags = {},
            bool enableSyncRep = false,
            uint64_t streamEndSeqno = ~0ull,
            ChangeStreams changeStreams = ChangeStreams::No);

    void createDcpObjects(const DcpProducerConfig& producerConfig,
                          const DcpStreamRequestConfig& streamRequestConfig);

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

    /**
     * This function (created for OSO tests) creates two collections (fruit
     * and vegetable) and calls writeTwoCollections
     *
     * @param endOnVegetable true and the last item written will be for the
     *        vegetable collection
     * @return current manifest and vbucket (::vbid) high-seqno
     */
    std::pair<CollectionsManifest, uint64_t> setupTwoCollections(
            bool endOnVegetable = false);

    /**
     * This function (created for OSO tests) writes to two collections (fruit
     * and vegetable). The keys are "a", "b", "c" and "d" to demonstrate the
     * lexicographical ordering of an OSO snapshot.
     *
     * @param endOnVegetable true and the last item written will be for the
     *        vegetable collection
     * @return vbucket (::vbid) high-seqno
     */
    uint64_t writeTwoCollectios(bool endOnTarget);

    static cb::engine_errc dcpAddFailoverLog(
            const std::vector<vbucket_failover_t>&);

    /**
     * Make a stream request value for requesting a set of collection IDs.
     * Returns a serialised JSON object of the form:
     *
     *     {"collections": ["a", "b", "c"] }
     *
     * For each of the collection IDs specified (e.g. 10, 11, 12).
     * @param collections Set of collection IDs to stream.
     */
    static std::string makeStreamRequestValue(
            std::initializer_list<CollectionID> collections);

    CookieIface* cookieC;
    CookieIface* cookieP;
    std::unique_ptr<CollectionsDcpTestProducers> producers;
    std::shared_ptr<MockDcpProducer> producer;
    std::shared_ptr<MockDcpConsumer> consumer;
    Vbid replicaVB;

    // Most tests in this testsuite setup in-memory DCP streams and assume that
    // checkpoints aren't eagerly removed through the test. This cursor is
    // registered to ensure that pre-condition, and then moved in some test
    // where necessary (eg, to allow checkpoint removal or expel).
    std::shared_ptr<CheckpointCursor> helperCursor;
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

    static auto allConfigValues() {
        auto configs = STParameterizedBucketTest::allConfigValues();
        return configs;
    };

    static auto persistentConfigValues() {
        auto configs = STParameterizedBucketTest::persistentConfigValues();
        return configs;
    };

protected:
    /**
     * Test what happens to the manifest when we promote a vBucket
     */
    void testVBPromotionUpdateManifest();

    /**
     * Test forced updates with replication
     */
    void replicateForcedUpdate(uint64_t newUid, bool warmup);

    void testMB_47009(uint64_t startSeqno,
                      snapshot_range_t snapshot,
                      snapshot_range_t expectedSnapshot);
};
