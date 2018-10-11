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

#include "test_helpers.h"

#include "../mock/mock_dcp.h"
#include "../mock/mock_stream.h"
#include "dcp/consumer.h"
#include "vbucket.h"

#include <gtest/gtest.h>

void handleProducerResponseIfStepBlocked(DcpConsumer& consumer,
                                         MockDcpMessageProducers& producers) {
    // MB-29441: Added a call to DcpConsumer::handleGetErrorMap() in
    // DcpConsumer::step().
    // We need to call DcpConsumer::handleResponse() to notify (set a flag)
    // that the GetErrorMap response has been received. The next calls to
    // step() would block forever in DcpConsumer::handleGetErrorMap() otherwise
    if (producers.last_op == cb::mcbp::ClientOpcode::GetErrorMap) {
        protocol_binary_response_header resp{};
        resp.response.setMagic(cb::mcbp::Magic::ClientResponse);
        resp.response.setOpcode(cb::mcbp::ClientOpcode::GetErrorMap);
        resp.response.setStatus(cb::mcbp::Status::Success);
        consumer.handleResponse(&resp);
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
                                         DocKeyEncodesCollectionId::No,
                                         nullptr,
                                         DcpStreamId{});

        // PassiveStream::processMutation does 2 things:
        //     1) setWithMeta; that enqueues the item into the
        //         - checkpoint; if the item is from a memory-snapshot, or from
        //             a disk-snapshot and vbHighSeqno > 0
        //         - backfill queue; if the item is from a disl-snapshot and
        //             vbHighSeqno = 0
        //     2) calls PassiveStream::handleSnapshotEnd (which must close the
        //         open checkpoint if the current mutation is the snapshot-end)
        ASSERT_EQ(ENGINE_SUCCESS, stream.processMutation(&mutation));
    }
}

std::unique_ptr<MutationConsumerMessage> makeMutationConsumerMessage(
        uint64_t seqno, Vbid vbid, const std::string& value, uint64_t opaque) {
    queued_item qi(new Item(makeStoredDocKey("key_" + std::to_string(seqno)),
                            0 /*flags*/,
                            0 /*expiry*/,
                            value.c_str(),
                            value.size(),
                            PROTOCOL_BINARY_RAW_BYTES,
                            0 /*cas*/,
                            seqno,
                            vbid));
    return std::make_unique<MutationConsumerMessage>(
            std::move(qi),
            opaque,
            IncludeValue::Yes,
            IncludeXattrs::Yes,
            IncludeDeleteTime::No,
            DocKeyEncodesCollectionId::No,
            nullptr,
            DcpStreamId{});
}
