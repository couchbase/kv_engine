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
#include "dcp_expiration.h"
#include "engine_wrapper.h"
#include "utilities.h"
#include "../../mcbp.h"

void dcp_expiration_executor(McbpConnection* c, void* packet) {
    auto* req = reinterpret_cast<protocol_binary_request_dcp_expiration*>(packet);
    ENGINE_ERROR_CODE ret = c->getAiostat();
    c->setAiostat(ENGINE_SUCCESS);
    c->setEwouldblock(false);

    if (ret == ENGINE_SUCCESS) {
        // Collection aware DCP will be sending the collection_len field
        auto body_offset =
                protocol_binary_request_dcp_deletion::getHeaderLength(
                        c->isDcpCollectionAware());
        const uint16_t nkey = ntohs(req->message.header.request.keylen);
        const DocKey key{req->bytes + body_offset,
                         nkey,
                         c->getDocNamespaceForDcpMessage(
                                 req->message.body.collection_len)};
        const auto opaque = req->message.header.request.opaque;
        const auto datatype = req->message.header.request.datatype;
        const uint64_t cas = ntohll(req->message.header.request.cas);
        const uint16_t vbucket = ntohs(req->message.header.request.vbucket);
        const uint64_t by_seqno = ntohll(req->message.body.by_seqno);
        const uint64_t rev_seqno = ntohll(req->message.body.rev_seqno);
        const uint16_t nmeta = ntohs(req->message.body.nmeta);
        const uint32_t valuelen = ntohl(req->message.header.request.bodylen) -
                                  nkey - req->message.header.request.extlen -
                                  nmeta;
        cb::const_byte_buffer value{req->bytes + body_offset + nkey, valuelen};
        cb::const_byte_buffer meta{value.buf + valuelen, nmeta};
        uint32_t priv_bytes = 0;
        if (mcbp::datatype::is_xattr(datatype)) {
            priv_bytes = valuelen;
        }

        if (priv_bytes > COUCHBASE_MAX_ITEM_PRIVILEGED_BYTES) {
            ret = ENGINE_E2BIG;
        } else {
            ret = c->getBucketEngine()->dcp.expiration(c->getBucketEngineAsV0(),
                                                       c->getCookie(), opaque,
                                                       key, value, priv_bytes,
                                                       datatype, cas, vbucket,
                                                       by_seqno, rev_seqno,
                                                       meta);
        }
    }

    switch (ret) {
    case ENGINE_SUCCESS:
        c->setState(McbpStateMachine::State::new_cmd);
        break;

    case ENGINE_DISCONNECT:
        c->setState(McbpStateMachine::State::closing);
        break;

    case ENGINE_EWOULDBLOCK:
        c->setEwouldblock(true);
        break;

    default:
        mcbp_write_packet(c, engine_error_2_mcbp_protocol_error(ret));
    }
}

ENGINE_ERROR_CODE dcp_message_expiration(const void* void_cookie,
                                         uint32_t opaque,
                                         item* it,
                                         uint16_t vbucket,
                                         uint64_t by_seqno,
                                         uint64_t rev_seqno,
                                         const void* meta,
                                         uint16_t nmeta,
                                         uint8_t collection_len) {
    /*
     * EP engine don't use expiration, so we won't have tests for this
     * code. Add it back once we have people calling the method
     */
    auto* c = cookie2mcbp(void_cookie, __func__);
    cb::unique_item_ptr item(it, cb::ItemDeleter{c->getBucketEngineAsV0()});
    return ENGINE_ENOTSUP;
}
