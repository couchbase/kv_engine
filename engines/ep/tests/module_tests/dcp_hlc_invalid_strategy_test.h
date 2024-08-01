/*
 *     Copyright 2024-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#pragma once

#include "dcp_stream_test.h"
#include "test_helpers.h"

#include "../mock/mock_dcp_consumer.h"
#include "../mock/mock_stream.h"

/*
 * PassiveStream tests for operations with an Invalid CAS values and varying
 * dcp_hlc_invalid_cas_strategy congifs.
 */
class PassiveStreamHlcInvalidStrategyTest
    : virtual public SingleThreadedActiveStreamTest {
public:
    void SetUp() override {
        config_string += "dcp_hlc_invalid_strategy=ignore;";
        SingleThreadedKVBucketTest::SetUp();
        // replica required for DCP streams
        setVBucketState(vbid, vbucket_state_replica);
    }

protected:
    cb::engine_errc testDcpReplicationWithInvalidCas(queued_item item) {
        auto consumer = std::make_shared<MockDcpConsumer>(
                *engine, cookie, "test-consumer");
        auto passiveStream = std::static_pointer_cast<MockPassiveStream>(
                consumer->makePassiveStream(
                        *engine,
                        consumer,
                        "test-passive-stream",
                        {} /* flags */,
                        0 /* opaque */,
                        vbid,
                        0 /* startSeqno */,
                        std::numeric_limits<uint64_t>::max() /* endSeqno */,
                        0 /* vbUuid */,
                        0 /* snapStartSeqno */,
                        0 /* snapEndSeqno */,
                        0 /* vb_high_seqno */,
                        Collections::ManifestUid{} /* vb_manifest_uid */));
        SnapshotMarker snapshotMarker(
                0 /* opaque */,
                vbid,
                1,
                1,
                cb::mcbp::request::DcpSnapshotMarkerFlag::None,
                {} /*HCS*/,
                {} /*maxVisibleSeqno*/,
                {} /*SID*/);
        passiveStream->processMarker(&snapshotMarker);

        MutationConsumerMessage mutation(item,
                                         0 /* opaque */,
                                         IncludeValue::Yes,
                                         IncludeXattrs::Yes,
                                         IncludeDeleteTime::No,
                                         IncludeDeletedUserXattrs::Yes,
                                         DocKeyEncodesCollectionId::No,
                                         nullptr,
                                         cb::mcbp::DcpStreamId{});

        // KVBucket->setWithMeta/delteWithMeta/prepare code paths are hit
        // depending on the type of Item passed in: Commited/Deleted/Pending
        // Items respectively.
        return passiveStream->processMutation(&mutation);
    }
};
