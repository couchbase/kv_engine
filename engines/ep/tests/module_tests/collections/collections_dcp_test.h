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

#include "tests/module_tests/evp_store_single_threaded_test.h"

class MockDcpConsumer;
class MockDcpMessageProducers;
class MockDcpProducer;

class CollectionsDcpTest : public SingleThreadedKVBucketTest {
public:
    CollectionsDcpTest();

    // Setup a producer/consumer ready for the test
    void SetUp() override;
    Collections::VB::PersistedManifest getManifest(Vbid vb) const;

    void createDcpStream(boost::optional<cb::const_char_buffer> collections);

    void createDcpConsumer();

    void createDcpObjects(boost::optional<cb::const_char_buffer> collections);

    void TearDown() override;

    void teardown();

    void runCheckpointProcessor();

    void notifyAndStepToCheckpoint(
            cb::mcbp::ClientOpcode expectedOp =
                    cb::mcbp::ClientOpcode::DcpSnapshotMarker,
            bool fromMemory = true);

    void testDcpCreateDelete(int expectedCreates,
                             int expectedDeletes,
                             int expectedMutations,
                             bool fromMemory = true);

    void resetEngineAndWarmup(std::string new_config = "");

    static Vbid replicaVB;
    static std::shared_ptr<MockDcpConsumer> consumer;

    /*
     * DCP callback method to push SystemEvents on to the consumer
     */
    static ENGINE_ERROR_CODE sendSystemEvent(gsl::not_null<const void*> cookie,
                                             uint32_t opaque,
                                             Vbid vbucket,
                                             mcbp::systemevent::id event,
                                             uint64_t bySeqno,
                                             cb::const_byte_buffer key,
                                             cb::const_byte_buffer eventData);

    static ENGINE_ERROR_CODE dcpAddFailoverLog(
            vbucket_failover_t* entry,
            size_t nentries,
            gsl::not_null<const void*> cookie);

    const void* cookieC;
    const void* cookieP;
    std::unique_ptr<MockDcpMessageProducers> producers;
    std::shared_ptr<MockDcpProducer> producer;
};