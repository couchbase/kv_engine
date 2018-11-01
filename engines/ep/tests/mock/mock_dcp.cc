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

#include "config.h"

#include <relaxed_atomic.h>
#include <stdlib.h>

#include "collections/collections_types.h"
#include "item.h"
#include "mock_dcp.h"

#include <memcached/protocol_binary.h>

cb::mcbp::ClientOpcode dcp_last_op;
cb::mcbp::Status dcp_last_status;
uint8_t dcp_last_nru;
Vbid dcp_last_vbucket;
uint32_t dcp_last_opaque;
uint32_t dcp_last_flags;
uint32_t dcp_last_stream_opaque;
uint32_t dcp_last_locktime;
uint32_t dcp_last_packet_size;
uint64_t dcp_last_cas;
uint64_t dcp_last_start_seqno;
uint64_t dcp_last_end_seqno;
uint64_t dcp_last_vbucket_uuid;
uint64_t dcp_last_snap_start_seqno;
uint64_t dcp_last_snap_end_seqno;
Couchbase::RelaxedAtomic<uint64_t> dcp_last_byseqno;
uint64_t dcp_last_revseqno;
CollectionID dcp_last_collection_id;
uint32_t dcp_last_delete_time;
std::string dcp_last_meta;
std::string dcp_last_value;
std::string dcp_last_key;
vbucket_state_t dcp_last_vbucket_state;
protocol_binary_datatype_t dcp_last_datatype;
mcbp::systemevent::id dcp_last_system_event;

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

ENGINE_ERROR_CODE MockDcpMessageProducers::stream_req(uint32_t opaque,
                                                      Vbid vbucket,
                                                      uint32_t flags,
                                                      uint64_t start_seqno,
                                                      uint64_t end_seqno,
                                                      uint64_t vbucket_uuid,
                                                      uint64_t snap_start_seqno,
                                                      uint64_t snap_end_seqno) {
    clear_dcp_data();
    dcp_last_op = cb::mcbp::ClientOpcode::DcpStreamReq;
    dcp_last_opaque = opaque;
    dcp_last_vbucket = vbucket;
    dcp_last_flags = flags;
    dcp_last_start_seqno = start_seqno;
    dcp_last_end_seqno = end_seqno;
    dcp_last_vbucket_uuid = vbucket_uuid;
    dcp_last_packet_size = 64;
    dcp_last_snap_start_seqno = snap_start_seqno;
    dcp_last_snap_end_seqno = snap_end_seqno;
    return ENGINE_SUCCESS;
}

ENGINE_ERROR_CODE MockDcpMessageProducers::add_stream_rsp(
        uint32_t opaque, uint32_t stream_opaque, cb::mcbp::Status status) {
    clear_dcp_data();
    dcp_last_op = cb::mcbp::ClientOpcode::DcpAddStream;
    dcp_last_opaque = opaque;
    dcp_last_stream_opaque = stream_opaque;
    dcp_last_status = status;
    dcp_last_packet_size = 28;
    return ENGINE_SUCCESS;
}

ENGINE_ERROR_CODE MockDcpMessageProducers::marker_rsp(uint32_t opaque,
                                                      cb::mcbp::Status status) {
    clear_dcp_data();
    dcp_last_op = cb::mcbp::ClientOpcode::DcpSnapshotMarker;
    dcp_last_opaque = opaque;
    dcp_last_status = status;
    dcp_last_packet_size = 24;
    return ENGINE_SUCCESS;
}

ENGINE_ERROR_CODE MockDcpMessageProducers::set_vbucket_state_rsp(
        uint32_t opaque, cb::mcbp::Status status) {
    clear_dcp_data();
    dcp_last_op = cb::mcbp::ClientOpcode::DcpSetVbucketState;
    dcp_last_opaque = opaque;
    dcp_last_status = status;
    dcp_last_packet_size = 24;
    return ENGINE_SUCCESS;
}

ENGINE_ERROR_CODE MockDcpMessageProducers::stream_end(uint32_t opaque,
                                                      Vbid vbucket,
                                                      uint32_t flags) {
    clear_dcp_data();
    dcp_last_op = cb::mcbp::ClientOpcode::DcpStreamEnd;
    dcp_last_opaque = opaque;
    dcp_last_vbucket = vbucket;
    dcp_last_flags = flags;
    dcp_last_packet_size = 28;
    return ENGINE_SUCCESS;
}

ENGINE_ERROR_CODE MockDcpMessageProducers::marker(uint32_t opaque,
                                                  Vbid vbucket,
                                                  uint64_t snap_start_seqno,
                                                  uint64_t snap_end_seqno,
                                                  uint32_t flags) {
    clear_dcp_data();
    dcp_last_op = cb::mcbp::ClientOpcode::DcpSnapshotMarker;
    dcp_last_opaque = opaque;
    dcp_last_vbucket = vbucket;
    dcp_last_packet_size = 44;
    dcp_last_snap_start_seqno = snap_start_seqno;
    dcp_last_snap_end_seqno = snap_end_seqno;
    dcp_last_flags = flags;
    return ENGINE_SUCCESS;
}

ENGINE_ERROR_CODE MockDcpMessageProducers::mutation(uint32_t opaque,
                                                    item* itm,
                                                    Vbid vbucket,
                                                    uint64_t by_seqno,
                                                    uint64_t rev_seqno,
                                                    uint32_t lock_time,
                                                    const void* meta,
                                                    uint16_t nmeta,
                                                    uint8_t nru) {
    clear_dcp_data();
    Item* item = reinterpret_cast<Item*>(itm);
    dcp_last_op = cb::mcbp::ClientOpcode::DcpMutation;
    dcp_last_opaque = opaque;
    dcp_last_key.assign(item->getKey().c_str());
    dcp_last_vbucket = vbucket;
    dcp_last_byseqno = by_seqno;
    dcp_last_revseqno = rev_seqno;
    dcp_last_locktime = lock_time;
    dcp_last_meta.assign(static_cast<const char*>(meta), nmeta);
    dcp_last_value.assign(static_cast<const char*>(item->getData()),
                          item->getNBytes());
    dcp_last_nru = nru;

    // @todo: MB-24391: We are querying the header length with collections
    // off, which if we extended our testapp tests to do collections may not be
    // correct. For now collections testing is done via GTEST tests and isn't
    // reliant on dcp_last_packet_size so this doesn't cause any problems.
    dcp_last_packet_size =
            protocol_binary_request_dcp_mutation::getHeaderLength();
    dcp_last_packet_size = dcp_last_packet_size + dcp_last_key.length() +
                           item->getNBytes() + nmeta;

    dcp_last_datatype = item->getDataType();
    dcp_last_collection_id = item->getKey().getCollectionID();

    if (engine_handle_v1 && engine_handle) {
        engine_handle_v1->release(item);
    }
    return mutationStatus;
}

ENGINE_ERROR_CODE MockDcpMessageProducers::deletionInner(uint32_t opaque,
                                                         item* itm,
                                                         Vbid vbucket,
                                                         uint64_t by_seqno,
                                                         uint64_t rev_seqno,
                                                         const void* meta,
                                                         uint16_t nmeta,
                                                         uint32_t deleteTime,
                                                         uint32_t extlen) {
    clear_dcp_data();
    Item* item = reinterpret_cast<Item*>(itm);
    dcp_last_op = cb::mcbp::ClientOpcode::DcpDeletion;
    dcp_last_opaque = opaque;
    dcp_last_key.assign(item->getKey().c_str());
    dcp_last_cas = item->getCas();
    dcp_last_vbucket = vbucket;
    dcp_last_byseqno = by_seqno;
    dcp_last_revseqno = rev_seqno;
    dcp_last_meta.assign(static_cast<const char*>(meta), nmeta);

    // @todo: MB-24391 as above.
    dcp_last_packet_size = sizeof(protocol_binary_request_header) +
                           dcp_last_key.length() + item->getNBytes() + nmeta;
    dcp_last_packet_size += extlen;

    dcp_last_value.assign(static_cast<const char*>(item->getData()),
                          item->getNBytes());
    dcp_last_delete_time = deleteTime;
    dcp_last_collection_id = item->getKey().getCollectionID();

    if (engine_handle_v1 && engine_handle) {
        engine_handle_v1->release(item);
    }

    return ENGINE_SUCCESS;
}

ENGINE_ERROR_CODE MockDcpMessageProducers::deletion(uint32_t opaque,
                                                    item* itm,
                                                    Vbid vbucket,
                                                    uint64_t by_seqno,
                                                    uint64_t rev_seqno,
                                                    const void* meta,
                                                    uint16_t nmeta) {
    return deletionInner(opaque,
                         itm,
                         vbucket,
                         by_seqno,
                         rev_seqno,
                         meta,
                         nmeta,
                         0,
                         protocol_binary_request_dcp_deletion::extlen);
}

ENGINE_ERROR_CODE MockDcpMessageProducers::deletion_v2(uint32_t opaque,
                                                       gsl::not_null<item*> itm,
                                                       Vbid vbucket,
                                                       uint64_t by_seqno,
                                                       uint64_t rev_seqno,
                                                       uint32_t deleteTime) {
    return deletionInner(opaque,
                         itm,
                         vbucket,
                         by_seqno,
                         rev_seqno,
                         nullptr,
                         0,
                         deleteTime,
                         protocol_binary_request_dcp_deletion_v2::extlen);
}

ENGINE_ERROR_CODE MockDcpMessageProducers::expiration(
        uint32_t, item* itm, Vbid, uint64_t, uint64_t, const void*, uint16_t) {
    clear_dcp_data();
    if (engine_handle_v1 && engine_handle) {
        engine_handle_v1->release(itm);
    }
    return ENGINE_ENOTSUP;
}

ENGINE_ERROR_CODE MockDcpMessageProducers::set_vbucket_state(
        uint32_t opaque, Vbid vbucket, vbucket_state_t state) {
    clear_dcp_data();
    dcp_last_op = cb::mcbp::ClientOpcode::DcpSetVbucketState;
    dcp_last_opaque = opaque;
    dcp_last_vbucket = vbucket;
    dcp_last_vbucket_state = state;
    dcp_last_packet_size = 25;
    return ENGINE_SUCCESS;
}

ENGINE_ERROR_CODE MockDcpMessageProducers::noop(uint32_t opaque) {
    clear_dcp_data();
    dcp_last_op = cb::mcbp::ClientOpcode::DcpNoop;
    dcp_last_opaque = opaque;
    return ENGINE_SUCCESS;
}

ENGINE_ERROR_CODE MockDcpMessageProducers::buffer_acknowledgement(
        uint32_t opaque, Vbid vbucket, uint32_t buffer_bytes) {
    clear_dcp_data();
    dcp_last_op = cb::mcbp::ClientOpcode::DcpBufferAcknowledgement;
    dcp_last_opaque = opaque;
    dcp_last_vbucket = vbucket;
    return ENGINE_SUCCESS;
}

ENGINE_ERROR_CODE MockDcpMessageProducers::control(uint32_t opaque,
                                                   const void* key,
                                                   uint16_t nkey,
                                                   const void* value,
                                                   uint32_t nvalue) {
    clear_dcp_data();
    dcp_last_op = cb::mcbp::ClientOpcode::DcpControl;
    dcp_last_opaque = opaque;
    dcp_last_key.assign(static_cast<const char*>(key), nkey);
    dcp_last_value.assign(static_cast<const char*>(value), nvalue);
    return ENGINE_SUCCESS;
}

ENGINE_ERROR_CODE MockDcpMessageProducers::system_event(
        uint32_t opaque,
        Vbid vbucket,
        mcbp::systemevent::id event,
        uint64_t bySeqno,
        mcbp::systemevent::version version,
        cb::const_byte_buffer key,
        cb::const_byte_buffer eventData) {
    clear_dcp_data();
    dcp_last_op = cb::mcbp::ClientOpcode::DcpSystemEvent;
    dcp_last_system_event = event;
    if (event == mcbp::systemevent::id::CreateCollection ||
        event == mcbp::systemevent::id::DeleteCollection) {
        dcp_last_collection_id =
                reinterpret_cast<const Collections::SystemEventDcpData*>(
                        eventData.data())
                        ->cid.to_host();
    }
    return ENGINE_SUCCESS;
}

ENGINE_ERROR_CODE MockDcpMessageProducers::get_error_map(uint32_t opaque,
                                                         uint16_t version) {
    clear_dcp_data();
    dcp_last_op = cb::mcbp::ClientOpcode::GetErrorMap;
    return ENGINE_SUCCESS;
}

MockDcpMessageProducers::MockDcpMessageProducers(EngineIface* engine) {
    engine_handle = engine;
    engine_handle_v1 = engine;
}

void MockDcpMessageProducers::setMutationStatus(ENGINE_ERROR_CODE code) {
    mutationStatus = code;
}

void clear_dcp_data() {
    dcp_last_op = cb::mcbp::ClientOpcode::Invalid;
    dcp_last_status = cb::mcbp::Status::Success;
    dcp_last_nru = 0;
    dcp_last_vbucket = Vbid(0);
    dcp_last_opaque = 0;
    dcp_last_flags = 0;
    dcp_last_stream_opaque = 0;
    dcp_last_locktime = 0;
    dcp_last_cas = 0;
    dcp_last_start_seqno = 0;
    dcp_last_end_seqno = 0;
    dcp_last_vbucket_uuid = 0;
    dcp_last_snap_start_seqno = 0;
    dcp_last_snap_end_seqno = 0;
    dcp_last_byseqno = 0;
    dcp_last_meta.clear();
    dcp_last_value.clear();
    dcp_last_key.clear();
    dcp_last_vbucket_state = (vbucket_state_t)0;
    dcp_last_delete_time = 0;
    dcp_last_collection_id = 0;
}
