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

#include "msg_producers_border_guard.h"
#include "objectregistry.h"

#include <memcached/durability_spec.h>
#include <memcached/engine.h>

DcpMsgProducersBorderGuard::DcpMsgProducersBorderGuard(
        dcp_message_producers& guarded)
    : guarded(guarded) {
}

ENGINE_ERROR_CODE DcpMsgProducersBorderGuard::get_failover_log(uint32_t opaque,
                                                               Vbid vbucket) {
    NonBucketAllocationGuard guard;
    return guarded.get_failover_log(opaque, vbucket);
}
ENGINE_ERROR_CODE DcpMsgProducersBorderGuard::stream_req(
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

ENGINE_ERROR_CODE DcpMsgProducersBorderGuard::add_stream_rsp(
        uint32_t opaque, uint32_t stream_opaque, cb::mcbp::Status status) {
    NonBucketAllocationGuard guard;
    return guarded.add_stream_rsp(opaque, stream_opaque, status);
}
ENGINE_ERROR_CODE DcpMsgProducersBorderGuard::marker_rsp(
        uint32_t opaque, cb::mcbp::Status status) {
    NonBucketAllocationGuard guard;
    return guarded.marker_rsp(opaque, status);
}
ENGINE_ERROR_CODE DcpMsgProducersBorderGuard::set_vbucket_state_rsp(
        uint32_t opaque, cb::mcbp::Status status) {
    NonBucketAllocationGuard guard;
    return guarded.set_vbucket_state_rsp(opaque, status);
}
ENGINE_ERROR_CODE DcpMsgProducersBorderGuard::stream_end(
        uint32_t opaque,
        Vbid vbucket,
        uint32_t flags,
        cb::mcbp::DcpStreamId sid) {
    NonBucketAllocationGuard guard;
    return guarded.stream_end(opaque, vbucket, flags, sid);
}
ENGINE_ERROR_CODE DcpMsgProducersBorderGuard::marker(
        uint32_t opaque,
        Vbid vbucket,
        uint64_t start_seqno,
        uint64_t end_seqno,
        uint32_t flags,
        boost::optional<uint64_t> highCompletedSeqno,
        boost::optional<uint64_t> maxVisibleSeqno,
        cb::mcbp::DcpStreamId sid) {
    NonBucketAllocationGuard guard;
    return guarded.marker(opaque,
                          vbucket,
                          start_seqno,
                          end_seqno,
                          flags,
                          highCompletedSeqno,
                          maxVisibleSeqno,
                          sid);
}
ENGINE_ERROR_CODE DcpMsgProducersBorderGuard::mutation(
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
ENGINE_ERROR_CODE DcpMsgProducersBorderGuard::deletion(
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
ENGINE_ERROR_CODE DcpMsgProducersBorderGuard::deletion_v2(
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
ENGINE_ERROR_CODE DcpMsgProducersBorderGuard::expiration(
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
ENGINE_ERROR_CODE DcpMsgProducersBorderGuard::set_vbucket_state(
        uint32_t opaque, Vbid vbucket, vbucket_state_t state) {
    NonBucketAllocationGuard guard;
    return guarded.set_vbucket_state(opaque, vbucket, state);
}
ENGINE_ERROR_CODE DcpMsgProducersBorderGuard::noop(uint32_t opaque) {
    NonBucketAllocationGuard guard;
    return guarded.noop(opaque);
}
ENGINE_ERROR_CODE DcpMsgProducersBorderGuard::buffer_acknowledgement(
        uint32_t opaque, Vbid vbucket, uint32_t buffer_bytes) {
    NonBucketAllocationGuard guard;
    return guarded.buffer_acknowledgement(opaque, vbucket, buffer_bytes);
}
ENGINE_ERROR_CODE DcpMsgProducersBorderGuard::control(
        uint32_t opaque,
        cb::const_char_buffer key,
        cb::const_char_buffer value) {
    NonBucketAllocationGuard guard;
    return guarded.control(opaque, key, value);
}
ENGINE_ERROR_CODE DcpMsgProducersBorderGuard::system_event(
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
ENGINE_ERROR_CODE DcpMsgProducersBorderGuard::get_error_map(uint32_t opaque,
                                                            uint16_t version) {
    NonBucketAllocationGuard guard;
    return guarded.get_error_map(opaque, version);
}
ENGINE_ERROR_CODE DcpMsgProducersBorderGuard::prepare(
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
ENGINE_ERROR_CODE DcpMsgProducersBorderGuard::seqno_acknowledged(
        uint32_t opaque, Vbid vbucket, uint64_t prepared_seqno) {
    NonBucketAllocationGuard guard;
    return guarded.seqno_acknowledged(opaque, vbucket, prepared_seqno);
}
ENGINE_ERROR_CODE DcpMsgProducersBorderGuard::commit(uint32_t opaque,
                                                     Vbid vbucket,
                                                     const DocKey& key,
                                                     uint64_t prepare_seqno,
                                                     uint64_t commit_seqno) {
    NonBucketAllocationGuard guard;
    return guarded.commit(opaque, vbucket, key, prepare_seqno, commit_seqno);
}
ENGINE_ERROR_CODE DcpMsgProducersBorderGuard::abort(uint32_t opaque,
                                                    Vbid vbucket,
                                                    const DocKey& key,
                                                    uint64_t prepared_seqno,
                                                    uint64_t abort_seqno) {
    NonBucketAllocationGuard guard;
    return guarded.abort(opaque, vbucket, key, prepared_seqno, abort_seqno);
}
