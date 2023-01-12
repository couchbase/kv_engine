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

#include <relaxed_atomic.h>

#include "collections/system_event_types.h"
#include "item.h"
#include "mock_dcp.h"

#include <memcached/protocol_binary.h>

std::vector<vbucket_failover_t> dcp_failover_log;

cb::engine_errc mock_dcp_add_failover_log(
        const std::vector<vbucket_failover_t>& entries) {
    dcp_failover_log = entries;
    return cb::engine_errc::success;
}

cb::engine_errc MockDcpMessageProducers::get_failover_log(uint32_t opaque,
                                                          Vbid vbucket) {
    clear_dcp_data();
    return cb::engine_errc::not_supported;
}

cb::engine_errc MockDcpMessageProducers::stream_req(
        uint32_t opaque,
        Vbid vbucket,
        uint32_t flags,
        uint64_t start_seqno,
        uint64_t end_seqno,
        uint64_t vbucket_uuid,
        uint64_t snap_start_seqno,
        uint64_t snap_end_seqno,
        const std::string& request_value) {
    clear_dcp_data();
    last_op = cb::mcbp::ClientOpcode::DcpStreamReq;
    last_opaque = opaque;
    last_vbucket = vbucket;
    last_flags = flags;
    last_start_seqno = start_seqno;
    last_end_seqno = end_seqno;
    last_vbucket_uuid = vbucket_uuid;
    last_packet_size = sizeof(cb::mcbp::Request) +
                       sizeof(cb::mcbp::request::DcpStreamReqPayload) +
                       request_value.size();
    last_snap_start_seqno = snap_start_seqno;
    last_snap_end_seqno = snap_end_seqno;
    last_collection_filter = request_value;
    return cb::engine_errc::success;
}

cb::engine_errc MockDcpMessageProducers::add_stream_rsp(
        uint32_t opaque, uint32_t stream_opaque, cb::mcbp::Status status) {
    clear_dcp_data();
    last_op = cb::mcbp::ClientOpcode::DcpAddStream;
    last_opaque = opaque;
    last_stream_opaque = stream_opaque;
    last_status = status;
    last_packet_size = sizeof(cb::mcbp::Response) +
                       sizeof(cb::mcbp::response::DcpAddStreamPayload);
    return cb::engine_errc::success;
}

cb::engine_errc MockDcpMessageProducers::marker_rsp(uint32_t opaque,
                                                    cb::mcbp::Status status) {
    clear_dcp_data();
    last_op = cb::mcbp::ClientOpcode::DcpSnapshotMarker;
    last_opaque = opaque;
    last_status = status;
    last_packet_size = sizeof(cb::mcbp::Response);
    return cb::engine_errc::success;
}

cb::engine_errc MockDcpMessageProducers::set_vbucket_state_rsp(
        uint32_t opaque, cb::mcbp::Status status) {
    clear_dcp_data();
    last_op = cb::mcbp::ClientOpcode::DcpSetVbucketState;
    last_opaque = opaque;
    last_status = status;
    last_packet_size = sizeof(cb::mcbp::Request);
    return cb::engine_errc::success;
}

cb::engine_errc MockDcpMessageProducers::stream_end(
        uint32_t opaque,
        Vbid vbucket,
        cb::mcbp::DcpStreamEndStatus status,
        cb::mcbp::DcpStreamId sid) {
    clear_dcp_data();
    last_op = cb::mcbp::ClientOpcode::DcpStreamEnd;
    last_opaque = opaque;
    last_vbucket = vbucket;
    last_end_status = status;
    last_packet_size = sizeof(cb::mcbp::Request) +
                       sizeof(cb::mcbp::request::DcpStreamEndPayload);
    if (sid) {
        last_stream_id = sid;
        last_packet_size += sizeof(cb::mcbp::DcpStreamIdFrameInfo);
    }

    return cb::engine_errc::success;
}

cb::engine_errc MockDcpMessageProducers::marker(
        uint32_t opaque,
        Vbid vbucket,
        uint64_t snap_start_seqno,
        uint64_t snap_end_seqno,
        uint32_t flags,
        std::optional<uint64_t> highCompletedSeqno,
        std::optional<uint64_t> maxVisibleSeqno,
        std::optional<uint64_t> timestamp,
        cb::mcbp::DcpStreamId sid) {
    clear_dcp_data();
    last_op = cb::mcbp::ClientOpcode::DcpSnapshotMarker;
    last_opaque = opaque;
    last_vbucket = vbucket;
    last_packet_size = sizeof(cb::mcbp::Request);
    if (highCompletedSeqno || maxVisibleSeqno) {
        last_packet_size +=
                sizeof(cb::mcbp::request::DcpSnapshotMarkerV2xPayload) +
                sizeof(cb::mcbp::request::DcpSnapshotMarkerV2_0Value);
    } else {
        last_packet_size +=
                sizeof(cb::mcbp::request::DcpSnapshotMarkerV1Payload);
    }

    last_snap_start_seqno = snap_start_seqno;
    last_snap_end_seqno = snap_end_seqno;
    last_flags = flags;
    if (sid) {
        last_packet_size += sizeof(cb::mcbp::DcpStreamIdFrameInfo);
        last_stream_id = sid;
    }
    last_high_completed_seqno = highCompletedSeqno;
    last_max_visible_seqno = maxVisibleSeqno;
    last_timestamp = timestamp;
    return cb::engine_errc::success;
}

cb::engine_errc MockDcpMessageProducers::mutation(uint32_t opaque,
                                                  cb::unique_item_ptr itm,
                                                  Vbid vbucket,
                                                  uint64_t by_seqno,
                                                  uint64_t rev_seqno,
                                                  uint32_t lock_time,
                                                  uint8_t nru,
                                                  cb::mcbp::DcpStreamId sid) {
    auto result = handleMutationOrPrepare(cb::mcbp::ClientOpcode::DcpMutation,
                                          opaque,
                                          std::move(itm),
                                          vbucket,
                                          by_seqno,
                                          rev_seqno,
                                          lock_time,
                                          {},
                                          nru,
                                          sid);
    return result;
}

cb::engine_errc MockDcpMessageProducers::handleMutationOrPrepare(
        cb::mcbp::ClientOpcode opcode,
        uint32_t opaque,
        cb::unique_item_ptr itm,
        Vbid vbucket,
        uint64_t by_seqno,
        uint64_t rev_seqno,
        uint32_t lock_time,
        std::string_view meta,
        uint8_t nru,
        cb::mcbp::DcpStreamId sid) {
    clear_dcp_data();
    Item* item = reinterpret_cast<Item*>(itm.get());
    last_op = opcode;
    last_opaque = opaque;
    last_key.assign(item->getKey().c_str());
    last_vbucket = vbucket;
    last_byseqno = by_seqno;
    last_revseqno = rev_seqno;
    last_locktime = lock_time;
    last_meta = meta;
    last_value.assign(item->getData(), item->getNBytes());
    last_nru = nru;
    last_stream_id = sid;
    last_packet_size =
            sizeof(cb::mcbp::Request) +
            (last_stream_id ? sizeof(cb::mcbp::DcpStreamIdFrameInfo) : 0) +
            sizeof(cb::mcbp::request::DcpMutationPayload) + item->getNBytes();
    if (!isCollectionsSupported) {
        last_packet_size +=
                item->getKey().makeDocKeyWithoutCollectionID().size();
    } else {
        last_packet_size += item->getKey().size();
    }

    last_datatype = item->getDataType();
    last_collection_id = item->getKey().getCollectionID();

    return mutationStatus;
}

cb::engine_errc MockDcpMessageProducers::deletionInner(
        uint32_t opaque,
        cb::unique_item_ptr itm,
        Vbid vbucket,
        uint64_t by_seqno,
        uint64_t rev_seqno,
        uint32_t deleteTime,
        uint32_t extlen,
        DeleteSource deleteSource,
        cb::mcbp::DcpStreamId sid) {
    clear_dcp_data();
    auto* item = reinterpret_cast<Item*>(itm.get());
    if (deleteSource == DeleteSource::TTL) {
        last_op = cb::mcbp::ClientOpcode::DcpExpiration;
    } else {
        last_op = cb::mcbp::ClientOpcode::DcpDeletion;
    }
    last_opaque = opaque;
    last_key.assign(item->getKey().c_str());
    last_cas = item->getCas();
    last_vbucket = vbucket;
    last_byseqno = by_seqno;
    last_revseqno = rev_seqno;

    last_packet_size = sizeof(cb::mcbp::Request) +
                       (sid ? sizeof(cb::mcbp::DcpStreamIdFrameInfo) : 0) +
                       item->getNBytes() + extlen;

    if (!isCollectionsSupported) {
        last_packet_size +=
                item->getKey().makeDocKeyWithoutCollectionID().size();
    } else {
        last_packet_size += item->getKey().size();
    }

    last_value.assign(static_cast<const char*>(item->getData()),
                      item->getNBytes());
    last_delete_time = deleteTime;
    last_collection_id = item->getKey().getCollectionID();

    last_stream_id = sid;

    return cb::engine_errc::success;
}

cb::engine_errc MockDcpMessageProducers::deletion(uint32_t opaque,
                                                  cb::unique_item_ptr itm,
                                                  Vbid vbucket,
                                                  uint64_t by_seqno,
                                                  uint64_t rev_seqno,
                                                  cb::mcbp::DcpStreamId sid) {
    return deletionInner(opaque,
                         std::move(itm),
                         vbucket,
                         by_seqno,
                         rev_seqno,
                         0,
                         sizeof(cb::mcbp::request::DcpDeletionV1Payload),
                         DeleteSource::Explicit,
                         sid);
}

cb::engine_errc MockDcpMessageProducers::deletion_v2(
        uint32_t opaque,
        cb::unique_item_ptr itm,
        Vbid vbucket,
        uint64_t by_seqno,
        uint64_t rev_seqno,
        uint32_t deleteTime,
        cb::mcbp::DcpStreamId sid) {
    return deletionInner(opaque,
                         std::move(itm),
                         vbucket,
                         by_seqno,
                         rev_seqno,
                         deleteTime,
                         sizeof(cb::mcbp::request::DcpDeletionV2Payload),
                         DeleteSource::Explicit,
                         sid);
}

cb::engine_errc MockDcpMessageProducers::expiration(uint32_t opaque,
                                                    cb::unique_item_ptr itm,
                                                    Vbid vbucket,
                                                    uint64_t by_seqno,
                                                    uint64_t rev_seqno,
                                                    uint32_t deleteTime,
                                                    cb::mcbp::DcpStreamId sid) {
    return deletionInner(opaque,
                         std::move(itm),
                         vbucket,
                         by_seqno,
                         rev_seqno,
                         deleteTime,
                         sizeof(cb::mcbp::request::DcpExpirationPayload),
                         DeleteSource::TTL,
                         sid);
}

cb::engine_errc MockDcpMessageProducers::set_vbucket_state(
        uint32_t opaque, Vbid vbucket, vbucket_state_t state) {
    clear_dcp_data();
    last_op = cb::mcbp::ClientOpcode::DcpSetVbucketState;
    last_opaque = opaque;
    last_vbucket = vbucket;
    last_vbucket_state = state;
    last_packet_size = sizeof(cb::mcbp::Response) +
                       sizeof(cb::mcbp::request::DcpSetVBucketState);
    return cb::engine_errc::success;
}

cb::engine_errc MockDcpMessageProducers::noop(uint32_t opaque) {
    clear_dcp_data();
    last_op = cb::mcbp::ClientOpcode::DcpNoop;
    last_opaque = opaque;
    last_packet_size = sizeof(cb::mcbp::Request);
    return cb::engine_errc::success;
}

cb::engine_errc MockDcpMessageProducers::buffer_acknowledgement(
        uint32_t opaque, Vbid vbucket, uint32_t buffer_bytes) {
    clear_dcp_data();
    last_op = cb::mcbp::ClientOpcode::DcpBufferAcknowledgement;
    last_opaque = opaque;
    last_vbucket = vbucket;
    last_packet_size = (sizeof(cb::mcbp::Request) +
                        sizeof(cb::mcbp::request::DcpBufferAckPayload));
    return cb::engine_errc::success;
}

cb::engine_errc MockDcpMessageProducers::control(uint32_t opaque,
                                                 std::string_view key,
                                                 std::string_view value) {
    clear_dcp_data();
    last_op = cb::mcbp::ClientOpcode::DcpControl;
    last_opaque = opaque;
    last_key.assign(key.data(), key.size());
    last_value.assign(value.data(), value.size());
    last_packet_size = sizeof(cb::mcbp::Request) + key.size() + value.size();
    return cb::engine_errc::success;
}

cb::engine_errc MockDcpMessageProducers::system_event(
        uint32_t opaque,
        Vbid vbucket,
        mcbp::systemevent::id event,
        uint64_t bySeqno,
        mcbp::systemevent::version version,
        cb::const_byte_buffer key,
        cb::const_byte_buffer eventData,
        cb::mcbp::DcpStreamId sid) {
    clear_dcp_data();
    last_op = cb::mcbp::ClientOpcode::DcpSystemEvent;
    last_opaque = opaque;
    last_vbucket = vbucket;
    last_byseqno = bySeqno;
    last_system_event = event;
    last_system_event_data.insert(
            last_system_event_data.begin(), eventData.begin(), eventData.end());
    last_system_event_version = version;

    if (event == mcbp::systemevent::id::CreateCollection) {
        last_collection_id =
                reinterpret_cast<const Collections::CreateEventDcpData*>(
                        eventData.data())
                        ->cid.to_host();
        last_scope_id =
                reinterpret_cast<const Collections::CreateEventDcpData*>(
                        eventData.data())
                        ->sid.to_host();

        last_key.assign(reinterpret_cast<const char*>(key.data()), key.size());
    } else if (event == mcbp::systemevent::id::DeleteCollection) {
        last_collection_id =
                reinterpret_cast<const Collections::DropEventDcpData*>(
                        eventData.data())
                        ->cid.to_host();
    }

    last_packet_size = sizeof(cb::mcbp::Request) +
                       sizeof(cb::mcbp::request::DcpSystemEventPayload) +
                       key.size() + eventData.size();
    if (sid) {
        last_packet_size += sizeof(cb::mcbp::DcpStreamIdFrameInfo);
    }

    last_stream_id = sid;
    return cb::engine_errc::success;
}

cb::engine_errc MockDcpMessageProducers::prepare(uint32_t opaque,
                                                 cb::unique_item_ptr itm,
                                                 Vbid vbucket,
                                                 uint64_t by_seqno,
                                                 uint64_t rev_seqno,
                                                 uint32_t lock_time,
                                                 uint8_t nru,
                                                 DocumentState document_state,
                                                 cb::durability::Level) {
    return handleMutationOrPrepare(cb::mcbp::ClientOpcode::DcpPrepare,
                                   opaque,
                                   std::move(itm),
                                   vbucket,
                                   by_seqno,
                                   rev_seqno,
                                   lock_time,
                                   {},
                                   nru,
                                   {});
}

cb::engine_errc MockDcpMessageProducers::seqno_acknowledged(
        uint32_t opaque, Vbid vbucket, uint64_t prepared_seqno) {
    clear_dcp_data();
    last_op = cb::mcbp::ClientOpcode::DcpSeqnoAcknowledged;
    last_opaque = opaque;
    last_vbucket = vbucket;
    last_prepared_seqno = prepared_seqno;
    last_packet_size = (sizeof(cb::mcbp::Request) +
                        sizeof(cb::mcbp::request::DcpBufferAckPayload));
    return cb::engine_errc::success;
}

cb::engine_errc MockDcpMessageProducers::commit(uint32_t opaque,
                                                Vbid vbucket,
                                                const DocKey& key,
                                                uint64_t prepare_seqno,
                                                uint64_t commit_seqno) {
    clear_dcp_data();
    last_op = cb::mcbp::ClientOpcode::DcpCommit;
    last_opaque = opaque;
    last_vbucket = vbucket;
    last_prepared_seqno = prepare_seqno;
    last_commit_seqno = commit_seqno;
    last_key.assign(key.to_string());
    last_packet_size = sizeof(protocol_binary_request_header) +
                       sizeof(cb::mcbp::request::DcpCommitPayload);
    if (!isCollectionsSupported) {
        last_packet_size += key.makeDocKeyWithoutCollectionID().size();
    } else {
        last_packet_size += key.size();
    }
    return cb::engine_errc::success;
}

cb::engine_errc MockDcpMessageProducers::abort(uint32_t opaque,
                                               Vbid vbucket,
                                               const DocKey& key,
                                               uint64_t prepared_seqno,
                                               uint64_t abort_seqno) {
    clear_dcp_data();
    last_op = cb::mcbp::ClientOpcode::DcpAbort;
    last_opaque = opaque;
    last_vbucket = vbucket;
    last_prepared_seqno = prepared_seqno;
    last_abort_seqno = abort_seqno;
    last_key.assign(key.to_string());
    last_packet_size = sizeof(protocol_binary_request_header) +
                       sizeof(cb::mcbp::request::DcpAbortPayload);
    if (!isCollectionsSupported) {
        last_packet_size += key.makeDocKeyWithoutCollectionID().size();
    } else {
        last_packet_size += key.size();
    }
    return cb::engine_errc::success;
}

cb::engine_errc MockDcpMessageProducers::oso_snapshot(
        uint32_t opaque,
        Vbid vbucket,
        uint32_t flags,
        cb::mcbp::DcpStreamId sid) {
    clear_dcp_data();
    last_op = cb::mcbp::ClientOpcode::DcpOsoSnapshot;
    last_opaque = opaque;
    last_vbucket = vbucket;
    last_oso_snapshot_flags = flags;
    last_stream_id = sid;
    cb::mcbp::request::DcpOsoSnapshotPayload extras(flags);
    size_t totalBytes = sizeof(cb::mcbp::Request) + sizeof(extras);

    if (sid) {
        totalBytes += sizeof(cb::mcbp::DcpStreamIdFrameInfo);
    }
    last_packet_size = totalBytes;
    return cb::engine_errc::success;
}

cb::engine_errc MockDcpMessageProducers::seqno_advanced(
        uint32_t opaque,
        Vbid vbucket,
        uint64_t seqno,
        cb::mcbp::DcpStreamId sid) {
    clear_dcp_data();
    last_op = cb::mcbp::ClientOpcode::DcpSeqnoAdvanced;
    last_opaque = opaque;
    last_vbucket = vbucket;
    last_byseqno = seqno;
    cb::mcbp::request::DcpSeqnoAdvancedPayload extras(seqno);
    size_t totalBytes = sizeof(cb::mcbp::Request) + sizeof(extras);
    if (sid) {
        last_stream_id = sid;
        totalBytes += sizeof(cb::mcbp::DcpStreamIdFrameInfo);
    }
    last_packet_size = totalBytes;
    return cb::engine_errc::success;
}

cb::engine_errc MockDcpMessageProducers::get_error_map(uint32_t opaque,
                                                       uint16_t version) {
    clear_dcp_data();
    last_op = cb::mcbp::ClientOpcode::GetErrorMap;
    return cb::engine_errc::success;
}

void MockDcpMessageProducers::setMutationStatus(cb::engine_errc code) {
    mutationStatus = code;
}

void MockDcpMessageProducers::clear_dcp_data() {
    last_op = cb::mcbp::ClientOpcode::Invalid;
    last_status = cb::mcbp::Status::Success;
    last_nru = 0;
    last_vbucket = Vbid(0);
    last_opaque = 0;
    last_flags = 0;
    last_stream_opaque = 0;
    last_locktime = 0;
    last_cas = 0;
    last_start_seqno = 0;
    last_end_seqno = 0;
    last_vbucket_uuid = 0;
    last_snap_start_seqno = 0;
    last_snap_end_seqno = 0;
    last_byseqno = 0;
    last_meta.clear();
    last_value.clear();
    last_key.clear();
    last_vbucket_state = (vbucket_state_t)0;
    last_delete_time = 0;
    last_collection_id = 0;
    last_system_event_data.clear();
    last_system_event_version = mcbp::systemevent::version::version0;
    last_collection_manifest_uid = 0;
    last_stream_id = cb::mcbp::DcpStreamId{};
    last_prepared_seqno = 0;
    last_high_completed_seqno = 0;
    last_commit_seqno = 0;
    last_abort_seqno = 0;
    last_end_status = cb::mcbp::DcpStreamEndStatus::Ok;
    last_high_completed_seqno = std::nullopt;
    last_max_visible_seqno = std::nullopt;
    last_can_deduplicate = CanDeduplicate::Yes;
}
