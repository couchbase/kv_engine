/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2013-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#pragma once

#include <dcp/dcp-types.h>
#include <mcbp/protocol/opcode.h>
#include <memcached/dcp.h>
#include <memcached/engine.h>

#include <relaxed_atomic.h>

extern std::vector<vbucket_failover_t> dcp_failover_log;

cb::engine_errc mock_dcp_add_failover_log(
        const std::vector<vbucket_failover_t>& entries);

class MockDcpMessageProducers : public DcpMessageProducersIface {
public:
    cb::engine_errc get_failover_log(uint32_t opaque, Vbid vbucket) override;

    cb::engine_errc stream_req(uint32_t opaque,
                               Vbid vbucket,
                               uint32_t flags,
                               uint64_t start_seqno,
                               uint64_t end_seqno,
                               uint64_t vbucket_uuid,
                               uint64_t snap_start_seqno,
                               uint64_t snap_end_seqno,
                               const std::string& request_value) override;

    cb::engine_errc add_stream_rsp(uint32_t opaque,
                                   uint32_t stream_opaque,
                                   cb::mcbp::Status status) override;

    cb::engine_errc marker_rsp(uint32_t opaque,
                               cb::mcbp::Status status) override;

    cb::engine_errc set_vbucket_state_rsp(uint32_t opaque,
                                          cb::mcbp::Status status) override;

    cb::engine_errc stream_end(uint32_t opaque,
                               Vbid vbucket,
                               cb::mcbp::DcpStreamEndStatus status,
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

    cb::engine_errc expiration(uint32_t opaque,
                               cb::unique_item_ptr itm,
                               Vbid vbucket,
                               uint64_t by_seqno,
                               uint64_t rev_seqno,
                               uint32_t delete_time,
                               cb::mcbp::DcpStreamId sid) override;

    cb::engine_errc set_vbucket_state(uint32_t opaque,
                                      Vbid vbucket,
                                      vbucket_state_t state) override;
    cb::engine_errc noop(uint32_t opaque) override;
    cb::engine_errc buffer_acknowledgement(uint32_t opaque,
                                           Vbid vbucket,
                                           uint32_t buffer_bytes) override;
    cb::engine_errc control(uint32_t opaque,
                            std::string_view key,
                            std::string_view value) override;
    cb::engine_errc get_error_map(uint32_t opaque, uint16_t version) override;
    // Change the status code returned from mutation() to the specified value.
    void setMutationStatus(cb::engine_errc code);

    cb::engine_errc system_event(uint32_t opaque,
                                 Vbid vbucket,
                                 mcbp::systemevent::id event,
                                 uint64_t bySeqno,
                                 mcbp::systemevent::version version,
                                 cb::const_byte_buffer key,
                                 cb::const_byte_buffer eventData,
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

    cb::engine_errc seqno_acknowledged(uint32_t opaque,
                                       Vbid vbucket,
                                       uint64_t prepared_seqno) override;

    cb::engine_errc commit(uint32_t opaque,
                           Vbid vbucket,
                           const DocKey& key,
                           uint64_t prepare_seqno,
                           uint64_t commit_seqno) override;

    cb::engine_errc abort(uint32_t opaque,
                          Vbid vbucket,
                          const DocKey& key,
                          uint64_t prepared_seqno,
                          uint64_t abort_seqno) override;

    cb::engine_errc oso_snapshot(uint32_t opaque,
                                 Vbid vbucket,
                                 uint32_t flags,
                                 cb::mcbp::DcpStreamId sid) override;

    cb::engine_errc seqno_advanced(uint32_t opaque,
                                   Vbid vbucket,
                                   uint64_t seqno,
                                   cb::mcbp::DcpStreamId sid) override;

    void clear_dcp_data();

    cb::mcbp::ClientOpcode last_op;
    cb::mcbp::Status last_status;
    uint8_t last_nru;
    Vbid last_vbucket;
    uint32_t last_opaque;
    uint32_t last_flags;
    uint32_t last_stream_opaque;
    uint32_t last_locktime;
    uint32_t last_packet_size{0};
    uint64_t last_cas;
    uint64_t last_start_seqno;
    uint64_t last_end_seqno;
    uint64_t last_vbucket_uuid;
    uint64_t last_snap_start_seqno;
    uint64_t last_snap_end_seqno;
    cb::RelaxedAtomic<uint64_t> last_byseqno;
    uint64_t last_revseqno;
    CollectionID last_collection_id;
    ScopeID last_scope_id;
    uint32_t last_delete_time;
    std::string last_meta;
    std::string last_value;
    std::string last_key;
    vbucket_state_t last_vbucket_state;
    protocol_binary_datatype_t last_datatype;
    mcbp::systemevent::id last_system_event;
    std::vector<uint8_t> last_system_event_data;
    mcbp::systemevent::version last_system_event_version;
    uint64_t last_collection_manifest_uid;
    cb::mcbp::DcpStreamId last_stream_id;
    std::string last_collection_filter;
    uint64_t last_prepared_seqno;
    uint64_t last_high_completed_seqno;
    std::optional<uint64_t> last_timestamp;
    uint64_t last_commit_seqno;
    uint64_t last_abort_seqno;
    uint32_t last_oso_snapshot_flags;
    cb::mcbp::DcpStreamEndStatus last_end_status =
            cb::mcbp::DcpStreamEndStatus::Ok;

    bool isCollectionsSupported = false;

protected:
    /// Helper method for deletion / deletion_v2 / expiration
    cb::engine_errc deletionInner(uint32_t opaque,
                                  cb::unique_item_ptr itm,
                                  Vbid vbucket,
                                  uint64_t by_seqno,
                                  uint64_t rev_seqno,
                                  uint32_t deleteTime,
                                  uint32_t extlen,
                                  DeleteSource deleteSource,
                                  cb::mcbp::DcpStreamId sid);

    cb::engine_errc mutationStatus = cb::engine_errc::success;

    cb::engine_errc handleMutationOrPrepare(cb::mcbp::ClientOpcode opcode,
                                            uint32_t opaque,
                                            cb::unique_item_ptr itm,
                                            Vbid vbucket,
                                            uint64_t by_seqno,
                                            uint64_t rev_seqno,
                                            uint32_t lock_time,
                                            std::string_view meta,
                                            uint8_t nru,
                                            cb::mcbp::DcpStreamId sid);
};
