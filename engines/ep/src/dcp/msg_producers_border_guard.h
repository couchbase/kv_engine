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

#include <memcached/dcp.h>

/**
 * A class which wraps a given instance of DcpMessageProducersIface, and
 * "guards" calls to all methods of that instance by switching away from the
 * current thread's engine, calling the underlying guarded method, and then
 * switching back.
 *
 * This ensures that any memory allocations performed within any of the
 * interfaces' methods are correctly accounted (to the "Non Bucket").
 */
class DcpMsgProducersBorderGuard : public DcpMessageProducersIface {
public:
    explicit DcpMsgProducersBorderGuard(DcpMessageProducersIface& guarded);

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
                           std::optional<uint64_t> highCompletedSeqno,
                           std::optional<uint64_t> maxVisibleSeqno,
                           std::optional<uint64_t> timestamp,
                           cb::mcbp::DcpStreamId sid) override;

    cb::engine_errc mutation(uint32_t opaque,
                             cb::unique_item_ptr,
                             Vbid vbucket,
                             uint64_t by_seqno,
                             uint64_t rev_seqno,
                             uint32_t lock_time,
                             uint8_t nru,
                             cb::mcbp::DcpStreamId sid) override;

    cb::engine_errc deletion(uint32_t opaque,
                             cb::unique_item_ptr,
                             Vbid vbucket,
                             uint64_t by_seqno,
                             uint64_t rev_seqno,
                             cb::mcbp::DcpStreamId sid) override;

    cb::engine_errc deletion_v2(uint32_t opaque,
                                cb::unique_item_ptr,
                                Vbid vbucket,
                                uint64_t by_seqno,
                                uint64_t rev_seqno,
                                uint32_t delete_time,
                                cb::mcbp::DcpStreamId sid) override;

    cb::engine_errc expiration(uint32_t opaque,
                               cb::unique_item_ptr,
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

    cb::engine_errc system_event(uint32_t opaque,
                                 Vbid vbucket,
                                 mcbp::systemevent::id event,
                                 uint64_t bySeqno,
                                 mcbp::systemevent::version version,
                                 cb::const_byte_buffer key,
                                 cb::const_byte_buffer eventData,
                                 cb::mcbp::DcpStreamId sid) override;

    cb::engine_errc get_error_map(uint32_t opaque, uint16_t version) override;

    cb::engine_errc prepare(uint32_t opaque,
                            cb::unique_item_ptr,
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

private:
    /// The DCP message producers we are guarding.
    DcpMessageProducersIface& guarded;
};
