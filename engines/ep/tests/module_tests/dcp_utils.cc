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

#include "test_helpers.h"

#include "../mock/mock_dcp.h"
#include "../mock/mock_dcp_consumer.h"
#include "../mock/mock_stream.h"
#include "dcp/consumer.h"
#include "dcp/response.h"

#include <folly/portability/GTest.h>

void handleProducerResponseIfStepBlocked(MockDcpConsumer& consumer,
                                         MockDcpMessageProducers& producers) {
    // MB-29441: Added a call to DcpConsumer::handleGetErrorMap() in
    // DcpConsumer::step().
    // We need to call DcpConsumer::handleResponse() to notify (set a flag)
    // that the GetErrorMap response has been received. The next calls to
    // step() would block forever in DcpConsumer::handleGetErrorMap() otherwise
    if (producers.last_op == cb::mcbp::ClientOpcode::GetErrorMap) {
        cb::mcbp::Response resp{};
        resp.setMagic(cb::mcbp::Magic::ClientResponse);
        resp.setOpcode(cb::mcbp::ClientOpcode::GetErrorMap);
        resp.setStatus(cb::mcbp::Status::Success);
        consumer.handleResponse(resp);
    }

    // Part of the Consumer-Producer negotiation happens over DCP_CONTROL and
    // introduces a blocking step.
    // The blocking DCP_CONTROL request is signed at Consumer by tracking the
    // opaque value sent to the Producer.
    // We need to simulate the Producer response (with the proper opaque),
    // the next calls to step() would block forever otherwise.
    if (producers.last_op == cb::mcbp::ClientOpcode::DcpControl) {
        if (consumer.isOpaqueBlockingDcpControl(producers.last_opaque)) {
            cb::mcbp::Response resp{};
            resp.setMagic(cb::mcbp::Magic::ClientResponse);
            resp.setOpcode(cb::mcbp::ClientOpcode::DcpControl);
            resp.setStatus(cb::mcbp::Status::Success);
            resp.setOpaque(producers.last_opaque);
            consumer.handleResponse(resp);
        }
    }
}

void processMutations(MockPassiveStream& stream,
                      const int64_t seqnoStart,
                      const int64_t seqnoEnd) {
    ASSERT_GE(seqnoEnd, seqnoStart);
    for (auto i = seqnoStart; i <= seqnoEnd; i++) {
        // Queue item
        queued_item qi(new Item(makeStoredDocKey("key_" + std::to_string(i)),
                                0 /*flags*/,
                                0 /*expiry*/,
                                "value",
                                5 /*valueSize*/,
                                PROTOCOL_BINARY_RAW_BYTES,
                                0 /*cas*/,
                                i /*bySeqno*/,
                                stream.getVBucket()));

        MutationConsumerMessage mutation(std::move(qi),
                                         0 /* opaque */,
                                         IncludeValue::Yes,
                                         IncludeXattrs::Yes,
                                         IncludeDeleteTime::No,
                                         IncludeDeletedUserXattrs::Yes,
                                         DocKeyEncodesCollectionId::No,
                                         nullptr,
                                         cb::mcbp::DcpStreamId{});

        // PassiveStream::processMutation does 2 things:
        //     1) setWithMeta; that enqueues the item into the
        //         - checkpoint; if the item is from a memory-snapshot, or from
        //             a disk-snapshot and vbHighSeqno > 0
        //         - backfill queue; if the item is from a disl-snapshot and
        //             vbHighSeqno = 0
        //     2) calls PassiveStream::handleSnapshotEnd (which must close the
        //         open checkpoint if the current mutation is the snapshot-end)
        ASSERT_EQ(cb::engine_errc::success, stream.processMutation(&mutation));
    }
}

std::unique_ptr<MutationConsumerMessage> makeMutationConsumerMessage(
        uint64_t seqno,
        Vbid vbid,
        const std::string& value,
        uint64_t opaque,
        std::optional<std::string> key,
        std::optional<cb::durability::Requirements> reqs,
        bool deletion,
        uint64_t revSeqno) {
    const auto key_ = key ? *key : "key_" + std::to_string(seqno);
    queued_item qi(new Item(makeStoredDocKey(key_),
                            0 /*flags*/,
                            0 /*expiry*/,
                            value.c_str(),
                            value.size(),
                            PROTOCOL_BINARY_RAW_BYTES,
                            0 /*cas*/,
                            seqno,
                            vbid,
                            revSeqno));
    if (reqs) {
        qi->setPendingSyncWrite(*reqs);
    }
    if (deletion) {
        qi->setDeleted(DeleteSource::Explicit);
    }
    return std::make_unique<MutationConsumerMessage>(
            std::move(qi),
            opaque,
            IncludeValue::Yes,
            IncludeXattrs::Yes,
            IncludeDeleteTime::No,
            IncludeDeletedUserXattrs::Yes,
            DocKeyEncodesCollectionId::No,
            nullptr,
            cb::mcbp::DcpStreamId{});
}

std::unique_ptr<MutationConsumerMessage> makeMutationConsumerMessage(
        uint64_t opaque,
        uint64_t seqno,
        Vbid vbid,
        const std::string& value,
        const std::string& key,
        CollectionID cid) {
    queued_item qi(new Item(makeStoredDocKey(key, cid),
                            0,
                            0,
                            value.c_str(),
                            value.size(),
                            PROTOCOL_BINARY_RAW_BYTES,
                            0,
                            seqno,
                            vbid,
                            1));
    return std::make_unique<MutationConsumerMessage>(
            std::move(qi),
            opaque,
            IncludeValue::Yes,
            IncludeXattrs::Yes,
            IncludeDeleteTime::No,
            IncludeDeletedUserXattrs::Yes,
            DocKeyEncodesCollectionId::No,
            nullptr,
            cb::mcbp::DcpStreamId{});
}