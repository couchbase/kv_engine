/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2019 Couchbase, Inc
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

#include <folly/portability/GMock.h>
#include <memcached/dcp.h>
#include <memcached/durability_spec.h>
#include <memcached/engine.h>

class Item;

/**
 * Mock of dcp_messsage_producers, using GoogleMock to mock.
 */
class GMockDcpMsgProducers : public dcp_message_producers {
public:
    MOCK_METHOD2(get_failover_log,
                 ENGINE_ERROR_CODE(uint32_t opaque, Vbid vbucket));

    MOCK_METHOD9(stream_req,
                 ENGINE_ERROR_CODE(uint32_t opaque,
                                   Vbid vbucket,
                                   uint32_t flags,
                                   uint64_t start_seqno,
                                   uint64_t end_seqno,
                                   uint64_t vbucket_uuid,
                                   uint64_t snap_start_seqno,
                                   uint64_t snap_end_seqno,
                                   const std::string& request_value));

    MOCK_METHOD3(add_stream_rsp,
                 ENGINE_ERROR_CODE(uint32_t opaque,
                                   uint32_t stream_opaque,
                                   cb::mcbp::Status status));

    MOCK_METHOD2(marker_rsp,
                 ENGINE_ERROR_CODE(uint32_t opaque, cb::mcbp::Status status));

    MOCK_METHOD2(set_vbucket_state_rsp,
                 ENGINE_ERROR_CODE(uint32_t opaque, cb::mcbp::Status status));

    MOCK_METHOD4(stream_end,
                 ENGINE_ERROR_CODE(uint32_t opaque,
                                   Vbid vbucket,
                                   uint32_t flags,
                                   cb::mcbp::DcpStreamId sid));

    MOCK_METHOD8(
            marker,
            ENGINE_ERROR_CODE(uint32_t opaque,
                              Vbid vbucket,
                              uint64_t start_seqno,
                              uint64_t end_seqno,
                              uint32_t flags,
                              boost::optional<uint64_t> high_completed_seqno,
                              boost::optional<uint64_t> maxVisibleSeqno,
                              cb::mcbp::DcpStreamId sid));

    MOCK_METHOD8(mutation,
                 ENGINE_ERROR_CODE(uint32_t opaque,
                                   Item* itm,
                                   Vbid vbucket,
                                   uint64_t by_seqno,
                                   uint64_t rev_seqno,
                                   uint32_t lock_time,
                                   uint8_t nru,
                                   cb::mcbp::DcpStreamId sid));

    MOCK_METHOD6(deletion,
                 ENGINE_ERROR_CODE(uint32_t opaque,
                                   Item* itm,
                                   Vbid vbucket,
                                   uint64_t by_seqno,
                                   uint64_t rev_seqno,
                                   cb::mcbp::DcpStreamId sid));

    MOCK_METHOD7(deletionV2,
                 ENGINE_ERROR_CODE(uint32_t opaque,
                                   Item* itm,
                                   Vbid vbucket,
                                   uint64_t by_seqno,
                                   uint64_t rev_seqno,
                                   uint32_t delete_time,
                                   cb::mcbp::DcpStreamId sid));

    MOCK_METHOD7(expiration,
                 ENGINE_ERROR_CODE(uint32_t opaque,
                                   Item* itm,
                                   Vbid vbucket,
                                   uint64_t by_seqno,
                                   uint64_t rev_seqno,
                                   uint32_t delete_time,
                                   cb::mcbp::DcpStreamId sid));

    MOCK_METHOD3(set_vbucket_state,
                 ENGINE_ERROR_CODE(uint32_t opaque,
                                   Vbid vbucket,
                                   vbucket_state_t state));

    MOCK_METHOD1(noop, ENGINE_ERROR_CODE(uint32_t opaque));

    MOCK_METHOD3(buffer_acknowledgement,
                 ENGINE_ERROR_CODE(uint32_t opaque,
                                   Vbid vbucket,
                                   uint32_t buffer_bytes));

    MOCK_METHOD3(control,
                 ENGINE_ERROR_CODE(uint32_t opaque,
                                   cb::const_char_buffer key,
                                   cb::const_char_buffer value));

    MOCK_METHOD8(system_event,
                 ENGINE_ERROR_CODE(uint32_t opaque,
                                   Vbid vbucket,
                                   mcbp::systemevent::id event,
                                   uint64_t bySeqno,
                                   mcbp::systemevent::version version,
                                   cb::const_byte_buffer key,
                                   cb::const_byte_buffer eventData,
                                   cb::mcbp::DcpStreamId sid));

    MOCK_METHOD2(get_error_map,
                 ENGINE_ERROR_CODE(uint32_t opaque, uint16_t version));

    MOCK_METHOD9(prepare,
                 ENGINE_ERROR_CODE(uint32_t opaque,
                                   Item* itm,
                                   Vbid vbucket,
                                   uint64_t by_seqno,
                                   uint64_t rev_seqno,
                                   uint32_t lock_time,
                                   uint8_t nru,
                                   DocumentState document_state,
                                   cb::durability::Level level));

    MOCK_METHOD3(seqno_acknowledged,
                 ENGINE_ERROR_CODE(uint32_t opaque,
                                   Vbid vbucket,
                                   uint64_t prepared_seqno));

    MOCK_METHOD5(commit,
                 ENGINE_ERROR_CODE(uint32_t opaque,
                                   Vbid vbucket,
                                   const DocKey& key,
                                   uint64_t prepare_seqno,
                                   uint64_t commit_seqno));

    MOCK_METHOD5(abort,
                 ENGINE_ERROR_CODE(uint32_t opaque,
                                   Vbid vbucket,
                                   const DocKey& key,
                                   uint64_t prepared_seqno,
                                   uint64_t abort_seqno));

    // Current version of GMock doesn't support move-only types (e.g.
    // std::unique_ptr) for mocked function arguments. Workaround directly
    // implementing the affected methods (without GMock) and have them delegate
    // to a GMock method which takes a raw Item ptr.
    ENGINE_ERROR_CODE mutation(uint32_t opaque,
                               cb::unique_item_ptr itm,
                               Vbid vbucket,
                               uint64_t by_seqno,
                               uint64_t rev_seqno,
                               uint32_t lock_time,
                               uint8_t nru,
                               cb::mcbp::DcpStreamId sid) {
        return mutation(opaque,
                        reinterpret_cast<Item*>(itm.get()),
                        vbucket,
                        by_seqno,
                        rev_seqno,
                        lock_time,
                        nru,
                        sid);
    }

    ENGINE_ERROR_CODE deletion(uint32_t opaque,
                               cb::unique_item_ptr itm,
                               Vbid vbucket,
                               uint64_t by_seqno,
                               uint64_t rev_seqno,
                               cb::mcbp::DcpStreamId sid) {
        return deletion(opaque,
                        reinterpret_cast<Item*>(itm.get()),
                        vbucket,
                        by_seqno,
                        rev_seqno,
                        sid);
    }

    ENGINE_ERROR_CODE deletion_v2(uint32_t opaque,
                                  cb::unique_item_ptr itm,
                                  Vbid vbucket,
                                  uint64_t by_seqno,
                                  uint64_t rev_seqno,
                                  uint32_t delete_time,
                                  cb::mcbp::DcpStreamId sid) {
        return deletionV2(opaque,
                          reinterpret_cast<Item*>(itm.get()),
                          vbucket,
                          by_seqno,
                          rev_seqno,
                          delete_time,
                          sid);
    }

    ENGINE_ERROR_CODE expiration(uint32_t opaque,
                                 cb::unique_item_ptr itm,
                                 Vbid vbucket,
                                 uint64_t by_seqno,
                                 uint64_t rev_seqno,
                                 uint32_t delete_time,
                                 cb::mcbp::DcpStreamId sid) {
        return expiration(opaque,
                          reinterpret_cast<Item*>(itm.get()),
                          vbucket,
                          by_seqno,
                          rev_seqno,
                          delete_time,
                          sid);
    }

    ENGINE_ERROR_CODE prepare(uint32_t opaque,
                              cb::unique_item_ptr itm,
                              Vbid vbucket,
                              uint64_t by_seqno,
                              uint64_t rev_seqno,
                              uint32_t lock_time,
                              uint8_t nru,
                              DocumentState document_state,
                              cb::durability::Level level) {
        return prepare(opaque,
                       reinterpret_cast<Item*>(itm.get()),
                       vbucket,
                       by_seqno,
                       rev_seqno,
                       lock_time,
                       nru,
                       document_state,
                       level);
    }
};
