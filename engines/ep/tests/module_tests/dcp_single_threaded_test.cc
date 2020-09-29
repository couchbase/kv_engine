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

#include "../mock/mock_dcp_producer.h"
#include "checkpoint_manager.h"
#include "dcp/consumer.h"
#include "evp_store_single_threaded_test.h"
#include "kv_bucket.h"

#include <programs/engine_testapp/mock_cookie.h>

class STDcpTest : public STParameterizedBucketTest {};

/*
 * The following tests that when the disk_backfill_queue configuration is
 * set to false on receiving a snapshot marker it does not move into the
 * backfill phase and the open checkpoint id does not get set to zero.  Also
 * checks that on receiving a subsequent snapshot marker we do not create a
 * second checkpoint.
 */
TEST_P(STDcpTest, test_not_using_backfill_queue) {
    // Make vbucket replica so can add passive stream
    setVBucketStateAndRunPersistTask(vbid, vbucket_state_replica);

    const void* cookie = create_mock_cookie(engine.get());
    auto& connMap = engine->getDcpConnMap();
    auto* consumer = connMap.newConsumer(cookie, "test_consumer");

    // Add passive stream
    ASSERT_EQ(ENGINE_SUCCESS,
              consumer->addStream(0 /*opaque*/, vbid, 0 /*flags*/));

    // Get the checkpointManager
    auto& manager =
            *(engine->getKVBucket()->getVBucket(vbid)->checkpointManager);

    EXPECT_EQ(0, manager.getOpenCheckpointId());

    // Send a snapshotMarker
    consumer->snapshotMarker(1 /*opaque*/,
                             vbid,
                             0 /*start_seqno*/,
                             1 /*end_seqno*/,
                             MARKER_FLAG_DISK,
                             0 /*HCS*/,
                             {} /*maxVisibleSeqno*/);

    // Even without a checkpoint flag the first disk checkpoint will bump the ID
    EXPECT_EQ(1, manager.getOpenCheckpointId());

    EXPECT_TRUE(engine->getKVBucket()
                        ->getVBucket(vbid)
                        ->isReceivingInitialDiskSnapshot());

    // Create a producer outside of the ConnMap so we don't have to create a new
    // cookie. We'll never add a stream to it so we won't have any ConnMap/Store
    // TearDown issues.
    auto producer = std::make_shared<DcpProducer>(
            *engine, cookie, "test_producer", 0 /*flags*/, false /*startTask*/);

    /*
     * StreamRequest should tmp fail due to the associated vbucket receiving
     * a disk snapshot.
     */
    uint64_t rollbackSeqno = 0;
    auto err = producer->streamRequest(0 /*flags*/,
                                       0 /*opaque*/,
                                       vbid,
                                       0 /*start_seqno*/,
                                       0 /*end_seqno*/,
                                       0 /*vb_uuid*/,
                                       0 /*snap_start*/,
                                       0 /*snap_end*/,
                                       &rollbackSeqno,
                                       fakeDcpAddFailoverLog,
                                       {});

    EXPECT_EQ(ENGINE_TMPFAIL, err);

    // Open checkpoint Id should not be effected.
    EXPECT_EQ(1, manager.getOpenCheckpointId());

    /* Send a mutation */
    const DocKey docKey{nullptr, 0, DocKeyEncodesCollectionId::No};
    EXPECT_EQ(ENGINE_SUCCESS,
              consumer->mutation(1 /*opaque*/,
                                 docKey,
                                 {}, // value
                                 0, // priv bytes
                                 PROTOCOL_BINARY_RAW_BYTES,
                                 0, // cas
                                 vbid,
                                 0, // flags
                                 1, // bySeqno
                                 0, // rev seqno
                                 0, // exptime
                                 0, // locktime
                                 {}, // meta
                                 0)); // nru

    // Have received the mutation and so have snapshot end.
    EXPECT_FALSE(engine->getKVBucket()
                         ->getVBucket(vbid)
                         ->isReceivingInitialDiskSnapshot());
    EXPECT_EQ(1, manager.getOpenCheckpointId());

    consumer->snapshotMarker(1 /*opaque*/,
                             vbid,
                             0 /*start_seqno*/,
                             0 /*end_seqno*/,
                             0 /*flags*/,
                             {} /*HCS*/,
                             {} /*maxVisibleSeqno*/);

    // A new opencheckpoint should no be opened
    EXPECT_EQ(1, manager.getOpenCheckpointId());

    // Close stream
    ASSERT_EQ(ENGINE_SUCCESS, consumer->closeStream(0 /*opaque*/, vbid));

    connMap.disconnect(cookie);
    EXPECT_FALSE(connMap.isDeadConnectionsEmpty());
    connMap.manageConnections();
    EXPECT_TRUE(connMap.isDeadConnectionsEmpty());
}

INSTANTIATE_TEST_SUITE_P(PersistentAndEphemeral,
                         STDcpTest,
                         STParameterizedBucketTest::allConfigValues());
