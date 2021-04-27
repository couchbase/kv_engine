/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2020-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#pragma once

#include "tests/mock/mock_dcp.h"

class MockDcpConsumer;

class CollectionsDcpTestProducers : public MockDcpMessageProducers {
public:
    ~CollectionsDcpTestProducers() override = default;

    cb::engine_errc system_event(uint32_t opaque,
                                 Vbid vbucket,
                                 mcbp::systemevent::id event,
                                 uint64_t bySeqno,
                                 mcbp::systemevent::version version,
                                 cb::const_byte_buffer key,
                                 cb::const_byte_buffer eventData,
                                 cb::mcbp::DcpStreamId sid) override;

    cb::engine_errc marker(uint32_t opaque,
                           Vbid vbucket,
                           uint64_t start_seqno,
                           uint64_t end_seqno,
                           uint32_t flags,
                           std::optional<uint64_t> high_completed_seqno,
                           std::optional<uint64_t> maxVisibleSeqno,
                           std::optional<uint64_t> timestamp,
                           cb::mcbp::DcpStreamId sid) override;

    cb::engine_errc mutation(uint32_t opaque,
                             cb::unique_item_ptr itm,
                             Vbid vbucket,
                             uint64_t by_seqno,
                             uint64_t rev_seqno,
                             uint32_t lock_time,
                             uint8_t nru,
                             cb::mcbp::DcpStreamId sid) override;

    cb::engine_errc deletion(uint32_t opaque,
                             cb::unique_item_ptr itm,
                             Vbid vbucket,
                             uint64_t by_seqno,
                             uint64_t rev_seqno,
                             cb::mcbp::DcpStreamId sid) override;

    cb::engine_errc deletion_v2(uint32_t opaque,
                                cb::unique_item_ptr itm,
                                Vbid vbucket,
                                uint64_t by_seqno,
                                uint64_t rev_seqno,
                                uint32_t delete_time,
                                cb::mcbp::DcpStreamId sid) override;

    cb::engine_errc prepare(uint32_t opaque,
                            cb::unique_item_ptr itm,
                            Vbid vbucket,
                            uint64_t by_seqno,
                            uint64_t rev_seqno,
                            uint32_t lock_time,
                            uint8_t nru,
                            DocumentState document_state,
                            cb::durability::Level level) override;

    cb::engine_errc commit(uint32_t opaque,
                           Vbid vbucket,
                           const DocKey& key,
                           uint64_t prepare_seqno,
                           uint64_t commit_seqno) override;

    MockDcpConsumer* consumer = nullptr;
    Vbid replicaVB;
};
