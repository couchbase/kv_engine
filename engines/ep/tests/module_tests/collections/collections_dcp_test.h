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

#include "tests/mock/mock_dcp.h"
#include "tests/module_tests/collections/test_manifest.h"
#include "tests/module_tests/evp_store_single_threaded_test.h"
#include <memcached/dcp_stream_id.h>

class MockDcpConsumer;
class MockDcpProducer;

class CollectionsDcpTestProducers : public MockDcpMessageProducers {
public:
    CollectionsDcpTestProducers(EngineIface* engine = nullptr)
        : MockDcpMessageProducers(engine) {
    }
    ~CollectionsDcpTestProducers() {
    }

    ENGINE_ERROR_CODE system_event(uint32_t opaque,
                                   Vbid vbucket,
                                   mcbp::systemevent::id event,
                                   uint64_t bySeqno,
                                   mcbp::systemevent::version version,
                                   cb::const_byte_buffer key,
                                   cb::const_byte_buffer eventData,
                                   cb::mcbp::DcpStreamId sid) override;

    MockDcpConsumer* consumer = nullptr;
    Vbid replicaVB;
};

class CollectionsDcpTest : virtual public SingleThreadedKVBucketTest {
public:
    CollectionsDcpTest();

    // Setup a producer/consumer ready for the test
    void SetUp() override;

    void internalSetUp();

    Collections::KVStore::Manifest getPersistedManifest(Vbid vb) const;

    void createDcpStream(
            boost::optional<cb::const_char_buffer> collections,
            Vbid id = Vbid(0),
            cb::engine_errc expectedError = cb::engine_errc::success);

    void createDcpConsumer();

    void createDcpObjects(boost::optional<cb::const_char_buffer> collections);

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
            const std::vector<ScopeEntry::Entry>& expectedScopeDrops = {});

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

    static ENGINE_ERROR_CODE dcpAddFailoverLog(
            vbucket_failover_t* entry,
            size_t nentries,
            gsl::not_null<const void*> cookie);

    const void* cookieC;
    const void* cookieP;
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
};
