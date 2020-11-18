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

#pragma once

#include "tests/mock/mock_dcp.h"

class MockDcpConsumer;

class CollectionsDcpTestProducers : public MockDcpMessageProducers {
public:
    ~CollectionsDcpTestProducers() override = default;

    ENGINE_ERROR_CODE system_event(uint32_t opaque,
                                   Vbid vbucket,
                                   mcbp::systemevent::id event,
                                   uint64_t bySeqno,
                                   mcbp::systemevent::version version,
                                   cb::const_byte_buffer key,
                                   cb::const_byte_buffer eventData,
                                   cb::mcbp::DcpStreamId sid) override;

    ENGINE_ERROR_CODE marker(uint32_t opaque,
                             Vbid vbucket,
                             uint64_t start_seqno,
                             uint64_t end_seqno,
                             uint32_t flags,
                             std::optional<uint64_t> high_completed_seqno,
                             std::optional<uint64_t> maxVisibleSeqno,
                             std::optional<uint64_t> timestamp,
                             cb::mcbp::DcpStreamId sid) override;



    ENGINE_ERROR_CODE mutation(uint32_t opaque,
                               cb::unique_item_ptr itm,
                               Vbid vbucket,
                               uint64_t by_seqno,
                               uint64_t rev_seqno,
                               uint32_t lock_time,
                               uint8_t nru,
                               cb::mcbp::DcpStreamId sid) override;

    ENGINE_ERROR_CODE prepare(uint32_t opaque,
                              cb::unique_item_ptr itm,
                              Vbid vbucket,
                              uint64_t by_seqno,
                              uint64_t rev_seqno,
                              uint32_t lock_time,
                              uint8_t nru,
                              DocumentState document_state,
                              cb::durability::Level level) override;

    ENGINE_ERROR_CODE commit(uint32_t opaque,
                             Vbid vbucket,
                             const DocKey& key,
                             uint64_t prepare_seqno,
                             uint64_t commit_seqno) override;

    MockDcpConsumer* consumer = nullptr;
    Vbid replicaVB;
};
