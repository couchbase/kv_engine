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

#include "msg_producers_border_guard.h"
#include "objectregistry.h"

#include <memcached/durability_spec.h>
#include <memcached/engine.h>

DcpMsgProducersBorderGuard::DcpMsgProducersBorderGuard(
        DcpMessageProducersIface& guarded)
    : guarded(guarded) {
}

cb::engine_errc DcpMsgProducersBorderGuard::get_failover_log(uint32_t opaque,
                                                             Vbid vbucket) {
    NonBucketAllocationGuard guard;
    return guarded.get_failover_log(opaque, vbucket);
}
cb::engine_errc DcpMsgProducersBorderGuard::stream_req(
        uint32_t opaque,
        Vbid vbucket,
        uint32_t flags,
        uint64_t start_seqno,
        uint64_t end_seqno,
        uint64_t vbucket_uuid,
        uint64_t snap_start_seqno,
        uint64_t snap_end_seqno,
        const std::string& request_value) {
    NonBucketAllocationGuard guard;
    return guarded.stream_req(opaque,
                              vbucket,
                              flags,
                              start_seqno,
                              end_seqno,
                              vbucket_uuid,
                              snap_start_seqno,
                              snap_end_seqno,
                              request_value);
}

cb::engine_errc DcpMsgProducersBorderGuard::add_stream_rsp(
        uint32_t opaque, uint32_t stream_opaque, cb::mcbp::Status status) {
    NonBucketAllocationGuard guard;
    return guarded.add_stream_rsp(opaque, stream_opaque, status);
}
cb::engine_errc DcpMsgProducersBorderGuard::marker_rsp(
        uint32_t opaque, cb::mcbp::Status status) {
    NonBucketAllocationGuard guard;
    return guarded.marker_rsp(opaque, status);
}
cb::engine_errc DcpMsgProducersBorderGuard::set_vbucket_state_rsp(
        uint32_t opaque, cb::mcbp::Status status) {
    NonBucketAllocationGuard guard;
    return guarded.set_vbucket_state_rsp(opaque, status);
}
cb::engine_errc DcpMsgProducersBorderGuard::stream_end(
        uint32_t opaque,
        Vbid vbucket,
        cb::mcbp::DcpStreamEndStatus status,
        cb::mcbp::DcpStreamId sid) {
    NonBucketAllocationGuard guard;
    return guarded.stream_end(opaque, vbucket, status, sid);
}
cb::engine_errc DcpMsgProducersBorderGuard::marker(
        uint32_t opaque,
        Vbid vbucket,
        uint64_t start_seqno,
        uint64_t end_seqno,
        uint32_t flags,
        std::optional<uint64_t> highCompletedSeqno,
        std::optional<uint64_t> maxVisibleSeqno,
        std::optional<uint64_t> timestamp,
        cb::mcbp::DcpStreamId sid) {
    NonBucketAllocationGuard guard;
    return guarded.marker(opaque,
                          vbucket,
                          start_seqno,
                          end_seqno,
                          flags,
                          highCompletedSeqno,
                          maxVisibleSeqno,
                          timestamp,
                          sid);
}
cb::engine_errc DcpMsgProducersBorderGuard::mutation(
        uint32_t opaque,
        cb::unique_item_ptr itm,
        Vbid vbucket,
        uint64_t by_seqno,
        uint64_t rev_seqno,
        uint32_t lock_time,
        uint8_t nru,
        cb::mcbp::DcpStreamId sid) {
    NonBucketAllocationGuard guard;
    return guarded.mutation(opaque,
                            std::move(itm),
                            vbucket,
                            by_seqno,
                            rev_seqno,
                            lock_time,
                            nru,
                            sid);
}
cb::engine_errc DcpMsgProducersBorderGuard::deletion(
        uint32_t opaque,
        cb::unique_item_ptr itm,
        Vbid vbucket,
        uint64_t by_seqno,
        uint64_t rev_seqno,
        cb::mcbp::DcpStreamId sid) {
    NonBucketAllocationGuard guard;
    return guarded.deletion(opaque,
                            std::move(itm),
                            vbucket,
                            by_seqno,
                            rev_seqno,
                            sid);
}
cb::engine_errc DcpMsgProducersBorderGuard::deletion_v2(
        uint32_t opaque,
        cb::unique_item_ptr itm,
        Vbid vbucket,
        uint64_t by_seqno,
        uint64_t rev_seqno,
        uint32_t delete_time,
        cb::mcbp::DcpStreamId sid) {
    NonBucketAllocationGuard guard;
    return guarded.deletion_v2(opaque,
                               std::move(itm),
                               vbucket,
                               by_seqno,
                               rev_seqno,
                               delete_time,
                               sid);
}
cb::engine_errc DcpMsgProducersBorderGuard::expiration(
        uint32_t opaque,
        cb::unique_item_ptr itm,
        Vbid vbucket,
        uint64_t by_seqno,
        uint64_t rev_seqno,
        uint32_t delete_time,
        cb::mcbp::DcpStreamId sid) {
    NonBucketAllocationGuard guard;
    return guarded.expiration(opaque,
                              std::move(itm),
                              vbucket,
                              by_seqno,
                              rev_seqno,
                              delete_time,
                              sid);
}
cb::engine_errc DcpMsgProducersBorderGuard::set_vbucket_state(
        uint32_t opaque, Vbid vbucket, vbucket_state_t state) {
    NonBucketAllocationGuard guard;
    return guarded.set_vbucket_state(opaque, vbucket, state);
}
cb::engine_errc DcpMsgProducersBorderGuard::noop(uint32_t opaque) {
    NonBucketAllocationGuard guard;
    return guarded.noop(opaque);
}
cb::engine_errc DcpMsgProducersBorderGuard::buffer_acknowledgement(
        uint32_t opaque, uint32_t buffer_bytes) {
    NonBucketAllocationGuard guard;
    return guarded.buffer_acknowledgement(opaque, buffer_bytes);
}
cb::engine_errc DcpMsgProducersBorderGuard::control(uint32_t opaque,
                                                    std::string_view key,
                                                    std::string_view value) {
    NonBucketAllocationGuard guard;
    return guarded.control(opaque, key, value);
}
cb::engine_errc DcpMsgProducersBorderGuard::system_event(
        uint32_t opaque,
        Vbid vbucket,
        mcbp::systemevent::id event,
        uint64_t bySeqno,
        mcbp::systemevent::version version,
        cb::const_byte_buffer key,
        cb::const_byte_buffer eventData,
        cb::mcbp::DcpStreamId sid) {
    NonBucketAllocationGuard guard;
    return guarded.system_event(
            opaque, vbucket, event, bySeqno, version, key, eventData, sid);
}
cb::engine_errc DcpMsgProducersBorderGuard::get_error_map(uint32_t opaque,
                                                          uint16_t version) {
    NonBucketAllocationGuard guard;
    return guarded.get_error_map(opaque, version);
}
cb::engine_errc DcpMsgProducersBorderGuard::prepare(
        uint32_t opaque,
        cb::unique_item_ptr itm,
        Vbid vbucket,
        uint64_t by_seqno,
        uint64_t rev_seqno,
        uint32_t lock_time,
        uint8_t nru,
        DocumentState document_state,
        cb::durability::Level level) {
    NonBucketAllocationGuard guard;
    return guarded.prepare(opaque,
                           std::move(itm),
                           vbucket,
                           by_seqno,
                           rev_seqno,
                           lock_time,
                           nru,
                           document_state,
                           level);
}
cb::engine_errc DcpMsgProducersBorderGuard::seqno_acknowledged(
        uint32_t opaque, Vbid vbucket, uint64_t prepared_seqno) {
    NonBucketAllocationGuard guard;
    return guarded.seqno_acknowledged(opaque, vbucket, prepared_seqno);
}
cb::engine_errc DcpMsgProducersBorderGuard::commit(uint32_t opaque,
                                                   Vbid vbucket,
                                                   const DocKey& key,
                                                   uint64_t prepare_seqno,
                                                   uint64_t commit_seqno) {
    NonBucketAllocationGuard guard;
    return guarded.commit(opaque, vbucket, key, prepare_seqno, commit_seqno);
}
cb::engine_errc DcpMsgProducersBorderGuard::abort(uint32_t opaque,
                                                  Vbid vbucket,
                                                  const DocKey& key,
                                                  uint64_t prepared_seqno,
                                                  uint64_t abort_seqno) {
    NonBucketAllocationGuard guard;
    return guarded.abort(opaque, vbucket, key, prepared_seqno, abort_seqno);
}

cb::engine_errc DcpMsgProducersBorderGuard::oso_snapshot(
        uint32_t opaque,
        Vbid vbucket,
        uint32_t flags,
        cb::mcbp::DcpStreamId sid) {
    NonBucketAllocationGuard guard;
    return guarded.oso_snapshot(opaque, vbucket, flags, sid);
}

cb::engine_errc DcpMsgProducersBorderGuard::seqno_advanced(
        uint32_t opaque,
        Vbid vbucket,
        uint64_t seqno,
        cb::mcbp::DcpStreamId sid) {
    NonBucketAllocationGuard guard;
    return guarded.seqno_advanced(opaque, vbucket, seqno, sid);
}
