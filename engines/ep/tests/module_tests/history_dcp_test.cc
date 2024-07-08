/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2023-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "collections/collections_dcp_test.h"
#include "tests/module_tests/dcp_producer_config.h"
#include "tests/module_tests/dcp_stream_request_config.h"
#include "tests/module_tests/test_helpers.h"

#include <memcached/protocol_binary.h>

using namespace cb::mcbp;

class HistoryDcpTest : public CollectionsDcpParameterizedTest {
public:
    void SetUp() override {
        // Configure a large history window. All of the tests which require
        // duplicates require that magma retains all of the test mutations.
        // MockMagmaKVStore then allows for arbitrary positioning of the history
        // window.
        config_string += "history_retention_bytes=104857600";

        CollectionsDcpParameterizedTest::SetUp();
    }
};

// Test that a seqno-advance snapshot is marked with history
TEST_P(HistoryDcpTest, SeqnoAdvanceSnapshot) {
    // Producer disables FlatBuffersEvents so that a modify history event is
    // replaced by a seqno advance message
    createDcpObjects(DcpProducerConfig{"SeqnoAdvanceSnapshot",
                                       OutOfOrderSnapshots::No,
                                       SyncReplication::No,
                                       ChangeStreams::Yes,
                                       IncludeXattrs::Yes,
                                       IncludeDeleteTime::Yes,
                                       FlatBuffersEvents::No},
                     // Request says start=1, recall that stream-request uses
                     // the "last received seqno" as input. So here start=1 will
                     // result in the backfill sending inclusive of 2.
                     DcpStreamRequestConfig{vbid,
                                            {}, // flags
                                            1, // opaque
                                            0, // start from beginning
                                            ~0ull, // no end
                                            0, // snap start 1
                                            0, // snap end 2
                                            0,
                                            std::string_view{},
                                            cb::engine_errc::success});

    // Create collection with history enable
    CollectionsManifest cm;
    setCollections(cookie,
                   cm.add(CollectionEntry::vegetable, cb::NoExpiryLimit, true));

    // no need to even flush in this test - all in-memory DCP.
    notifyAndStepToCheckpoint();
    stepAndExpect(ClientOpcode::DcpSystemEvent);

    // Generate modification
    setCollections(
            cookie,
            cm.update(CollectionEntry::vegetable, cb::NoExpiryLimit, {}));

    notifyAndStepToCheckpoint();

    EXPECT_TRUE(isFlagSet(producers->last_snapshot_marker_flags,
                          DcpSnapshotMarkerFlag::History));
    EXPECT_FALSE(isFlagSet(producers->last_snapshot_marker_flags,
                           DcpSnapshotMarkerFlag::Disk));
    stepAndExpect(ClientOpcode::DcpSeqnoAdvanced);
    EXPECT_EQ(2, producers->last_byseqno);
}

TEST_P(HistoryDcpTest, ManyModifications) {
    using namespace cb::mcbp;
    using namespace mcbp::systemevent;
    using namespace CollectionEntry;

    createDcpObjects(std::string_view{},
                     OutOfOrderSnapshots::Yes,
                     {},
                     true, // sync-repl enabled
                     ~0ull,
                     ChangeStreams::Yes);

    // Create fruit collection with noTTL but history=true
    CollectionsManifest cm;
    cm.add(CollectionEntry::fruit, cb::NoExpiryLimit, true);
    setCollections(cookie, cm);

    notifyAndStepToCheckpoint();
    EXPECT_TRUE(isFlagSet(producers->last_snapshot_marker_flags,
                          DcpSnapshotMarkerFlag::History));
    EXPECT_FALSE(isFlagSet(producers->last_snapshot_marker_flags,
                           DcpSnapshotMarkerFlag::Disk));

    stepAndExpect(ClientOpcode::DcpSystemEvent);
    EXPECT_EQ(producers->last_system_event, id::BeginCollection);
    EXPECT_EQ(producers->last_collection_id, fruit.getId());

    // Now modify the collection a few times.
    cm.update(CollectionEntry::fruit, std::chrono::seconds(1), true);
    setCollections(cookie, cm);
    cm.update(CollectionEntry::fruit, std::chrono::seconds(2), true);
    setCollections(cookie, cm);
    cm.update(CollectionEntry::fruit, std::chrono::seconds(3), true);
    setCollections(cookie, cm);

    // Validate the in-memory DCP stream.
    notifyAndStepToCheckpoint();
    EXPECT_TRUE(isFlagSet(producers->last_snapshot_marker_flags,
                          DcpSnapshotMarkerFlag::History));

    stepAndExpect(ClientOpcode::DcpSystemEvent);
    EXPECT_EQ(producers->last_system_event, id::ModifyCollection);
    EXPECT_EQ(producers->last_collection_id, fruit.getId());
    EXPECT_EQ(producers->last_max_ttl.value(), std::chrono::seconds(1));

    stepAndExpect(ClientOpcode::DcpSnapshotMarker);
    stepAndExpect(ClientOpcode::DcpSystemEvent);
    EXPECT_EQ(producers->last_system_event, id::ModifyCollection);
    EXPECT_EQ(producers->last_collection_id, fruit.getId());
    EXPECT_EQ(producers->last_max_ttl.value(), std::chrono::seconds(2));

    stepAndExpect(ClientOpcode::DcpSnapshotMarker);
    stepAndExpect(ClientOpcode::DcpSystemEvent);
    EXPECT_EQ(producers->last_system_event, id::ModifyCollection);
    EXPECT_EQ(producers->last_collection_id, fruit.getId());
    EXPECT_EQ(producers->last_max_ttl.value(), std::chrono::seconds(3));

    // Prior to fixing MB-57174 the modify events incorrectly de-duplicated, now
    // expect 4 to be flushed.
    flush_vbucket_to_disk(vbid, 4);

    // validate the backfill data.
    ensureDcpWillBackfill();

    // DCP stream with no filter - all collections visible.
    createDcpObjects(std::string_view{},
                     OutOfOrderSnapshots::No,
                     {},
                     true, // sync-repl enabled
                     ~0ull,
                     ChangeStreams::Yes);

    runBackfill();
    stepAndExpect(ClientOpcode::DcpSnapshotMarker);
    EXPECT_TRUE(isFlagSet(producers->last_snapshot_marker_flags,
                          DcpSnapshotMarkerFlag::History |
                                  DcpSnapshotMarkerFlag::MayContainDuplicates |
                                  DcpSnapshotMarkerFlag::Disk));

    stepAndExpect(ClientOpcode::DcpSystemEvent);
    EXPECT_EQ(producers->last_system_event, id::BeginCollection);
    EXPECT_EQ(producers->last_collection_id, fruit.getId());

    stepAndExpect(ClientOpcode::DcpSystemEvent);
    EXPECT_EQ(producers->last_system_event, id::ModifyCollection);
    EXPECT_EQ(producers->last_collection_id, fruit.getId());
    EXPECT_EQ(producers->last_max_ttl.value(), std::chrono::seconds(1));

    stepAndExpect(ClientOpcode::DcpSystemEvent);
    EXPECT_EQ(producers->last_system_event, id::ModifyCollection);
    EXPECT_EQ(producers->last_collection_id, fruit.getId());
    EXPECT_EQ(producers->last_max_ttl.value(), std::chrono::seconds(2));

    stepAndExpect(ClientOpcode::DcpSystemEvent);
    EXPECT_EQ(producers->last_system_event, id::ModifyCollection);
    EXPECT_EQ(producers->last_collection_id, fruit.getId());
    EXPECT_EQ(producers->last_max_ttl.value(), std::chrono::seconds(3));
}

INSTANTIATE_TEST_SUITE_P(HistoryDcpTests,
                         HistoryDcpTest,
                         STParameterizedBucketTest::magmaConfigValues(),
                         STParameterizedBucketTest::PrintToStringParamName);