/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2013 Couchbase, Inc
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

#include <relaxed_atomic.h>
#include <stdlib.h>

#include "collections/collections_types.h"
#include "item.h"
#include "mock_dcp.h"

#include <memcached/protocol_binary.h>

static EngineIface* engine_handle = nullptr;
static EngineIface* engine_handle_v1 = nullptr;

std::vector<std::pair<uint64_t, uint64_t> > dcp_failover_log;

ENGINE_ERROR_CODE mock_dcp_add_failover_log(vbucket_failover_t* entry,
                                            size_t nentries,
                                            gsl::not_null<const void*>) {
    while (!dcp_failover_log.empty()) {
        dcp_failover_log.clear();
    }

    if(nentries > 0) {
        for (size_t i = 0; i < nentries; i--) {
            std::pair<uint64_t, uint64_t> curr;
            curr.first = entry[i].uuid;
            curr.second = entry[i].seqno;
            dcp_failover_log.push_back(curr);
        }
    }
   return ENGINE_SUCCESS;
}

ENGINE_ERROR_CODE MockDcpMessageProducers::get_failover_log(uint32_t opaque,
                                                            Vbid vbucket) {
    clear_dcp_data();
    return ENGINE_ENOTSUP;
}

ENGINE_ERROR_CODE MockDcpMessageProducers::stream_req(
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
    last_packet_size = 64;
    last_snap_start_seqno = snap_start_seqno;
    last_snap_end_seqno = snap_end_seqno;
    last_collection_filter = request_value;
    return ENGINE_SUCCESS;
}

ENGINE_ERROR_CODE MockDcpMessageProducers::add_stream_rsp(
        uint32_t opaque, uint32_t stream_opaque, cb::mcbp::Status status) {
    clear_dcp_data();
    last_op = cb::mcbp::ClientOpcode::DcpAddStream;
    last_opaque = opaque;
    last_stream_opaque = stream_opaque;
    last_status = status;
    last_packet_size = 28;
    return ENGINE_SUCCESS;
}

ENGINE_ERROR_CODE MockDcpMessageProducers::marker_rsp(uint32_t opaque,
                                                      cb::mcbp::Status status) {
    clear_dcp_data();
    last_op = cb::mcbp::ClientOpcode::DcpSnapshotMarker;
    last_opaque = opaque;
    last_status = status;
    last_packet_size = 24;
    return ENGINE_SUCCESS;
}

ENGINE_ERROR_CODE MockDcpMessageProducers::set_vbucket_state_rsp(
        uint32_t opaque, cb::mcbp::Status status) {
    clear_dcp_data();
    last_op = cb::mcbp::ClientOpcode::DcpSetVbucketState;
    last_opaque = opaque;
    last_status = status;
    last_packet_size = 24;
    return ENGINE_SUCCESS;
}

ENGINE_ERROR_CODE MockDcpMessageProducers::stream_end(
        uint32_t opaque,
        Vbid vbucket,
        uint32_t flags,
        cb::mcbp::DcpStreamId sid) {
    clear_dcp_data();
    last_op = cb::mcbp::ClientOpcode::DcpStreamEnd;
    last_opaque = opaque;
    last_vbucket = vbucket;
    last_flags = flags;
    last_packet_size = 28;
    last_stream_id = sid;

    return ENGINE_SUCCESS;
}

ENGINE_ERROR_CODE MockDcpMessageProducers::marker(
        uint32_t opaque,
        Vbid vbucket,
        uint64_t snap_start_seqno,
        uint64_t snap_end_seqno,
        uint32_t flags,
        boost::optional<uint64_t> highCompletedSeqno,
        boost::optional<uint64_t> maxVisibleSeqno,
        cb::mcbp::DcpStreamId sid) {
    clear_dcp_data();
    last_op = cb::mcbp::ClientOpcode::DcpSnapshotMarker;
    last_opaque = opaque;
    last_vbucket = vbucket;
    last_packet_size = 44;
    last_snap_start_seqno = snap_start_seqno;
    last_snap_end_seqno = snap_end_seqno;
    last_flags = flags;
    last_stream_id = sid;
    if (highCompletedSeqno) {
        last_high_completed_seqno = *highCompletedSeqno;
    }
    return ENGINE_SUCCESS;
}

ENGINE_ERROR_CODE MockDcpMessageProducers::mutation(uint32_t opaque,
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
                                          nru);
    last_stream_id = sid;
    return result;
}

ENGINE_ERROR_CODE MockDcpMessageProducers::handleMutationOrPrepare(
        cb::mcbp::ClientOpcode opcode,
        uint32_t opaque,
        cb::unique_item_ptr itm,
        Vbid vbucket,
        uint64_t by_seqno,
        uint64_t rev_seqno,
        uint32_t lock_time,
        cb::const_char_buffer meta,
        uint8_t nru) {
    clear_dcp_data();
    Item* item = reinterpret_cast<Item*>(itm.get());
    last_op = opcode;
    last_opaque = opaque;
    last_key.assign(item->getKey().c_str());
    last_vbucket = vbucket;
    last_byseqno = by_seqno;
    last_revseqno = rev_seqno;
    last_locktime = lock_time;
    last_meta = to_string(meta);
    last_value.assign(item->getData(), item->getNBytes());
    last_nru = nru;

    // @todo: MB-24391: We are querying the header length with collections
    // off, which if we extended our testapp tests to do collections may not be
    // correct. For now collections testing is done via GTEST tests and isn't
    // reliant on last_packet_size so this doesn't cause any problems.
    last_packet_size = sizeof(cb::mcbp::Request) +
                       sizeof(cb::mcbp::request::DcpMutationPayload);
    last_packet_size = last_packet_size + last_key.length() +
                       item->getNBytes() + meta.size();

    last_datatype = item->getDataType();
    last_collection_id = item->getKey().getCollectionID();

    return mutationStatus;
}

ENGINE_ERROR_CODE MockDcpMessageProducers::deletionInner(
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

    // @todo: MB-24391 as above.
    last_packet_size = sizeof(protocol_binary_request_header) +
                       last_key.length() + item->getNBytes();
    last_packet_size += extlen;

    last_value.assign(static_cast<const char*>(item->getData()),
                      item->getNBytes());
    last_delete_time = deleteTime;
    last_collection_id = item->getKey().getCollectionID();

    last_stream_id = sid;

    return ENGINE_SUCCESS;
}

ENGINE_ERROR_CODE MockDcpMessageProducers::deletion(uint32_t opaque,
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

ENGINE_ERROR_CODE MockDcpMessageProducers::deletion_v2(
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

ENGINE_ERROR_CODE MockDcpMessageProducers::expiration(
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
                         sizeof(cb::mcbp::request::DcpExpirationPayload),
                         DeleteSource::TTL,
                         sid);
}

ENGINE_ERROR_CODE MockDcpMessageProducers::set_vbucket_state(
        uint32_t opaque, Vbid vbucket, vbucket_state_t state) {
    clear_dcp_data();
    last_op = cb::mcbp::ClientOpcode::DcpSetVbucketState;
    last_opaque = opaque;
    last_vbucket = vbucket;
    last_vbucket_state = state;
    last_packet_size = 25;
    return ENGINE_SUCCESS;
}

ENGINE_ERROR_CODE MockDcpMessageProducers::noop(uint32_t opaque) {
    clear_dcp_data();
    last_op = cb::mcbp::ClientOpcode::DcpNoop;
    last_opaque = opaque;
    return ENGINE_SUCCESS;
}

ENGINE_ERROR_CODE MockDcpMessageProducers::buffer_acknowledgement(
        uint32_t opaque, Vbid vbucket, uint32_t buffer_bytes) {
    clear_dcp_data();
    last_op = cb::mcbp::ClientOpcode::DcpBufferAcknowledgement;
    last_opaque = opaque;
    last_vbucket = vbucket;
    return ENGINE_SUCCESS;
}

ENGINE_ERROR_CODE MockDcpMessageProducers::control(
        uint32_t opaque,
        cb::const_char_buffer key,
        cb::const_char_buffer value) {
    clear_dcp_data();
    last_op = cb::mcbp::ClientOpcode::DcpControl;
    last_opaque = opaque;
    last_key.assign(key.data(), key.size());
    last_value.assign(value.data(), value.size());
    return ENGINE_SUCCESS;
}

ENGINE_ERROR_CODE MockDcpMessageProducers::system_event(
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

    last_stream_id = sid;
    return ENGINE_SUCCESS;
}

ENGINE_ERROR_CODE MockDcpMessageProducers::prepare(uint32_t opaque,
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
                                   nru);
}

ENGINE_ERROR_CODE MockDcpMessageProducers::seqno_acknowledged(
        uint32_t opaque, Vbid vbucket, uint64_t prepared_seqno) {
    last_op = cb::mcbp::ClientOpcode::DcpSeqnoAcknowledged;
    last_opaque = opaque;
    last_vbucket = vbucket;
    last_prepared_seqno = prepared_seqno;
    return ENGINE_SUCCESS;
}

ENGINE_ERROR_CODE MockDcpMessageProducers::commit(uint32_t opaque,
                                                  Vbid vbucket,
                                                  const DocKey& key,
                                                  uint64_t prepare_seqno,
                                                  uint64_t commit_seqno) {
    last_op = cb::mcbp::ClientOpcode::DcpCommit;
    last_opaque = opaque;
    last_vbucket = vbucket;
    last_prepared_seqno = prepare_seqno;
    last_commit_seqno = commit_seqno;
    return ENGINE_SUCCESS;
}

ENGINE_ERROR_CODE MockDcpMessageProducers::abort(uint32_t opaque,
                                                 Vbid vbucket,
                                                 const DocKey& key,
                                                 uint64_t prepared_seqno,
                                                 uint64_t abort_seqno) {
    last_op = cb::mcbp::ClientOpcode::DcpAbort;
    last_opaque = opaque;
    last_vbucket = vbucket;
    last_prepared_seqno = prepared_seqno;
    last_abort_seqno = abort_seqno;
    return ENGINE_SUCCESS;
}

ENGINE_ERROR_CODE MockDcpMessageProducers::oso_snapshot(
        uint32_t opaque,
        Vbid vbucket,
        uint32_t flags,
        cb::mcbp::DcpStreamId sid) {
    last_op = cb::mcbp::ClientOpcode::DcpOsoSnapshot;
    last_opaque = opaque;
    last_vbucket = vbucket;
    last_oso_snapshot_flags = flags;
    last_stream_id = sid;
    return ENGINE_SUCCESS;
}

ENGINE_ERROR_CODE MockDcpMessageProducers::get_error_map(uint32_t opaque,
                                                         uint16_t version) {
    clear_dcp_data();
    last_op = cb::mcbp::ClientOpcode::GetErrorMap;
    return ENGINE_SUCCESS;
}

MockDcpMessageProducers::MockDcpMessageProducers(EngineIface* engine) {
    engine_handle = engine;
    engine_handle_v1 = engine;
}

void MockDcpMessageProducers::setMutationStatus(ENGINE_ERROR_CODE code) {
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
}
