/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2017 Couchbase, Inc.
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
#include "dcp_deletion.h"
#include "engine_wrapper.h"
#include "utilities.h"
#include "../../mcbp.h"

static ENGINE_ERROR_CODE dcp_deletion_v1_executor(
        Cookie& cookie, const protocol_binary_request_dcp_deletion& request) {
    const uint16_t nkey = ntohs(request.message.header.request.keylen);
    // The V1 delete is not collection aware, so lock to the DefaultCollection
    const DocKey key{request.bytes + sizeof(request.bytes),
                     nkey,
                     DocKeyEncodesCollectionId::No};

    const auto opaque = request.message.header.request.opaque;
    const auto datatype = request.message.header.request.datatype;
    const uint64_t cas = ntohll(request.message.header.request.cas);
    const uint16_t vbucket = ntohs(request.message.header.request.vbucket);
    const uint64_t by_seqno = ntohll(request.message.body.by_seqno);
    const uint64_t rev_seqno = ntohll(request.message.body.rev_seqno);
    const uint16_t nmeta = ntohs(request.message.body.nmeta);
    const uint32_t valuelen = ntohl(request.message.header.request.bodylen) -
                              nkey - request.message.header.request.extlen -
                              nmeta;
    cb::const_byte_buffer value{request.bytes + sizeof(request.bytes) + nkey,
                                valuelen};
    cb::const_byte_buffer meta{value.buf + valuelen, nmeta};
    uint32_t priv_bytes = 0;
    if (mcbp::datatype::is_xattr(datatype)) {
        priv_bytes = valuelen;
    }
    if (priv_bytes <= COUCHBASE_MAX_ITEM_PRIVILEGED_BYTES) {
        return dcpDeletion(cookie,
                           opaque,
                           key,
                           value,
                           priv_bytes,
                           datatype,
                           cas,
                           vbucket,
                           by_seqno,
                           rev_seqno,
                           meta);
    } else {
        return ENGINE_E2BIG;
    }
    return ENGINE_DISCONNECT;
}

// The updated deletion sends no extended meta, but does send a deletion time
// and the collection_len
static ENGINE_ERROR_CODE dcp_deletion_v2_executor(
        Cookie& cookie,
        const protocol_binary_request_dcp_deletion_v2& request) {
    const uint16_t nkey = ntohs(request.message.header.request.keylen);
    auto& connection = cookie.getConnection();
    const auto key = connection.makeDocKey(
            {request.bytes + sizeof(request.bytes), nkey});

    const auto opaque = request.message.header.request.opaque;
    const auto datatype = request.message.header.request.datatype;
    const uint64_t cas = ntohll(request.message.header.request.cas);
    const uint16_t vbucket = ntohs(request.message.header.request.vbucket);
    const uint64_t by_seqno = ntohll(request.message.body.by_seqno);
    const uint64_t rev_seqno = ntohll(request.message.body.rev_seqno);
    const uint32_t delete_time = ntohl(request.message.body.delete_time);
    const uint32_t valuelen = ntohl(request.message.header.request.bodylen) -
                              nkey - request.message.header.request.extlen;
    cb::const_byte_buffer value{request.bytes + sizeof(request.bytes) + nkey,
                                valuelen};
    uint32_t priv_bytes = 0;
    if (mcbp::datatype::is_xattr(datatype)) {
        priv_bytes = valuelen;
    }

    if (priv_bytes <= COUCHBASE_MAX_ITEM_PRIVILEGED_BYTES) {
        return dcpDeletionV2(cookie,
                             opaque,
                             key,
                             value,
                             priv_bytes,
                             datatype,
                             cas,
                             vbucket,
                             by_seqno,
                             rev_seqno,
                             delete_time);
    } else {
        return ENGINE_E2BIG;
    }
}

void dcp_deletion_executor(Cookie& cookie) {
    auto packet = cookie.getPacket(Cookie::PacketContent::Full);

    auto& connection = cookie.getConnection();

    auto ret = cookie.swapAiostat(ENGINE_SUCCESS);

    if (ret == ENGINE_SUCCESS) {
        if (connection.isDcpDeleteV2()) {
            ret = dcp_deletion_v2_executor(
                    cookie,
                    *reinterpret_cast<
                            const protocol_binary_request_dcp_deletion_v2*>(
                            packet.data()));
        } else {
            ret = dcp_deletion_v1_executor(
                    cookie,
                    *reinterpret_cast<
                            const protocol_binary_request_dcp_deletion*>(
                            packet.data()));
        }
    }

    ret = connection.remapErrorCode(ret);
    switch (ret) {
    case ENGINE_SUCCESS:
        connection.setState(StateMachine::State::new_cmd);
        break;

    case ENGINE_DISCONNECT:
        connection.setState(StateMachine::State::closing);
        break;

    case ENGINE_EWOULDBLOCK:
        cookie.setEwouldblock(true);
        break;

    default:
        cookie.sendResponse(cb::engine_errc(ret));
    }
}
