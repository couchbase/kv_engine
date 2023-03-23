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
                                            0, // flags
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

    EXPECT_TRUE(producers->last_flags & MARKER_FLAG_HISTORY);
    EXPECT_FALSE(producers->last_flags & MARKER_FLAG_DISK);
    stepAndExpect(ClientOpcode::DcpSeqnoAdvanced);
    EXPECT_EQ(2, producers->last_byseqno);
}

INSTANTIATE_TEST_SUITE_P(HistoryDcpTests,
                         HistoryDcpTest,
                         STParameterizedBucketTest::magmaConfigValues(),
                         STParameterizedBucketTest::PrintToStringParamName);