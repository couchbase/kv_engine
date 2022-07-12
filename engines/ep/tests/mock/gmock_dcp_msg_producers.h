/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2019-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
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
class GMockDcpMsgProducers : public DcpMessageProducersIface {
public:
    MOCK_METHOD(cb::engine_errc,
                get_failover_log,
                (uint32_t opaque, Vbid vbucket),
                (override));

    MOCK_METHOD(cb::engine_errc,
                stream_req,
                (uint32_t opaque,
                 Vbid vbucket,
                 uint32_t flags,
                 uint64_t start_seqno,
                 uint64_t end_seqno,
                 uint64_t vbucket_uuid,
                 uint64_t snap_start_seqno,
                 uint64_t snap_end_seqno,
                 const std::string& request_value),
                (override));

    MOCK_METHOD(cb::engine_errc,
                add_stream_rsp,
                (uint32_t opaque,
                 uint32_t stream_opaque,
                 cb::mcbp::Status status),
                (override));

    MOCK_METHOD(cb::engine_errc,
                marker_rsp,
                (uint32_t opaque, cb::mcbp::Status status),
                (override));

    MOCK_METHOD(cb::engine_errc,
                set_vbucket_state_rsp,
                (uint32_t opaque, cb::mcbp::Status status),
                (override));

    MOCK_METHOD(cb::engine_errc,
                stream_end,
                (uint32_t opaque,
                 Vbid vbucket,
                 cb::mcbp::DcpStreamEndStatus status,
                 cb::mcbp::DcpStreamId sid),
                (override));

    MOCK_METHOD(cb::engine_errc,
                marker,
                (uint32_t opaque,
                 Vbid vbucket,
                 uint64_t start_seqno,
                 uint64_t end_seqno,
                 uint32_t flags,
                 std::optional<uint64_t> high_completed_seqno,
                 std::optional<uint64_t> maxVisibleSeqno,
                 std::optional<uint64_t> timestamp,
                 cb::mcbp::DcpStreamId sid),
                (override));

    MOCK_METHOD(cb::engine_errc,
                mutation,
                (uint32_t opaque,
                 Item* itm,
                 Vbid vbucket,
                 uint64_t by_seqno,
                 uint64_t rev_seqno,
                 uint32_t lock_time,
                 uint8_t nru,
                 cb::mcbp::DcpStreamId sid));

    MOCK_METHOD(cb::engine_errc,
                deletion,
                (uint32_t opaque,
                 Item* itm,
                 Vbid vbucket,
                 uint64_t by_seqno,
                 uint64_t rev_seqno,
                 cb::mcbp::DcpStreamId sid));

    MOCK_METHOD(cb::engine_errc,
                deletionV2,
                (uint32_t opaque,
                 Item* itm,
                 Vbid vbucket,
                 uint64_t by_seqno,
                 uint64_t rev_seqno,
                 uint32_t delete_time,
                 cb::mcbp::DcpStreamId sid));

    MOCK_METHOD(cb::engine_errc,
                expiration,
                (uint32_t opaque,
                 Item* itm,
                 Vbid vbucket,
                 uint64_t by_seqno,
                 uint64_t rev_seqno,
                 uint32_t delete_time,
                 cb::mcbp::DcpStreamId sid));

    MOCK_METHOD(cb::engine_errc,
                set_vbucket_state,
                (uint32_t opaque, Vbid vbucket, vbucket_state_t state),
                (override));

    MOCK_METHOD(cb::engine_errc, noop, (uint32_t opaque), (override));

    MOCK_METHOD(cb::engine_errc,
                buffer_acknowledgement,
                (uint32_t opaque, uint32_t buffer_bytes),
                (override));

    MOCK_METHOD(cb::engine_errc,
                control,
                (uint32_t opaque, std::string_view key, std::string_view value),
                (override));

    MOCK_METHOD(cb::engine_errc,
                system_event,
                (uint32_t opaque,
                 Vbid vbucket,
                 mcbp::systemevent::id event,
                 uint64_t bySeqno,
                 mcbp::systemevent::version version,
                 cb::const_byte_buffer key,
                 cb::const_byte_buffer eventData,
                 cb::mcbp::DcpStreamId sid),
                (override));

    MOCK_METHOD(cb::engine_errc,
                get_error_map,
                (uint32_t opaque, uint16_t version),
                (override));

    MOCK_METHOD(cb::engine_errc,
                prepare,
                (uint32_t opaque,
                 Item* itm,
                 Vbid vbucket,
                 uint64_t by_seqno,
                 uint64_t rev_seqno,
                 uint32_t lock_time,
                 uint8_t nru,
                 DocumentState document_state,
                 cb::durability::Level level));

    MOCK_METHOD(cb::engine_errc,
                seqno_acknowledged,
                (uint32_t opaque, Vbid vbucket, uint64_t prepared_seqno),
                (override));

    MOCK_METHOD(cb::engine_errc,
                commit,
                (uint32_t opaque,
                 Vbid vbucket,
                 const DocKey& key,
                 uint64_t prepare_seqno,
                 uint64_t commit_seqno),
                (override));

    MOCK_METHOD(cb::engine_errc,
                abort,
                (uint32_t opaque,
                 Vbid vbucket,
                 const DocKey& key,
                 uint64_t prepared_seqno,
                 uint64_t abort_seqno),
                (override));

    MOCK_METHOD(cb::engine_errc,
                oso_snapshot,
                (uint32_t opaque,
                 Vbid vbucket,
                 uint32_t flags,
                 cb::mcbp::DcpStreamId sid),
                (override));

    MOCK_METHOD(cb::engine_errc,
                seqno_advanced,
                (uint32_t opaque,
                 Vbid vbucket,
                 uint64_t prepared_seqno,
                 cb::mcbp::DcpStreamId sid),
                (override));

    // Current version of GMock doesn't support move-only types (e.g.
    // std::unique_ptr) for mocked function arguments. Workaround directly
    // implementing the affected methods (without GMock) and have them delegate
    // to a GMock method which takes a raw Item ptr.
    cb::engine_errc mutation(uint32_t opaque,
                             cb::unique_item_ptr itm,
                             Vbid vbucket,
                             uint64_t by_seqno,
                             uint64_t rev_seqno,
                             uint32_t lock_time,
                             uint8_t nru,
                             cb::mcbp::DcpStreamId sid) override {
        return mutation(opaque,
                        reinterpret_cast<Item*>(itm.get()),
                        vbucket,
                        by_seqno,
                        rev_seqno,
                        lock_time,
                        nru,
                        sid);
    }

    cb::engine_errc deletion(uint32_t opaque,
                             cb::unique_item_ptr itm,
                             Vbid vbucket,
                             uint64_t by_seqno,
                             uint64_t rev_seqno,
                             cb::mcbp::DcpStreamId sid) override {
        return deletion(opaque,
                        reinterpret_cast<Item*>(itm.get()),
                        vbucket,
                        by_seqno,
                        rev_seqno,
                        sid);
    }

    cb::engine_errc deletion_v2(uint32_t opaque,
                                cb::unique_item_ptr itm,
                                Vbid vbucket,
                                uint64_t by_seqno,
                                uint64_t rev_seqno,
                                uint32_t delete_time,
                                cb::mcbp::DcpStreamId sid) override {
        return deletionV2(opaque,
                          reinterpret_cast<Item*>(itm.get()),
                          vbucket,
                          by_seqno,
                          rev_seqno,
                          delete_time,
                          sid);
    }

    cb::engine_errc expiration(uint32_t opaque,
                               cb::unique_item_ptr itm,
                               Vbid vbucket,
                               uint64_t by_seqno,
                               uint64_t rev_seqno,
                               uint32_t delete_time,
                               cb::mcbp::DcpStreamId sid) override {
        return expiration(opaque,
                          reinterpret_cast<Item*>(itm.get()),
                          vbucket,
                          by_seqno,
                          rev_seqno,
                          delete_time,
                          sid);
    }

    cb::engine_errc prepare(uint32_t opaque,
                            cb::unique_item_ptr itm,
                            Vbid vbucket,
                            uint64_t by_seqno,
                            uint64_t rev_seqno,
                            uint32_t lock_time,
                            uint8_t nru,
                            DocumentState document_state,
                            cb::durability::Level level) override {
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
