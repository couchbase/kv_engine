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
#include "get_meta_context.h"
#include "engine_wrapper.h"

#include <daemon/debug_helpers.h>
#include <daemon/mcaudit.h>
#include <daemon/mcbp.h>
#include <xattr/utils.h>

GetMetaCommandContext::GetMetaCommandContext(
        McbpConnection& c, protocol_binary_request_get_meta* req)
    : SteppableCommandContext(c),
      key(req->bytes + sizeof(req->bytes) + req->message.header.request.extlen,
          ntohs(req->message.header.request.keylen),
          c.getDocNamespace()),
      vbucket(ntohs(req->message.header.request.vbucket)),
      state(State::GetItemMeta),
      info(),
      request(req),
      fetchDatatype(false) {
    // Read the version if extlen is 1
    if (request->message.header.request.extlen == 1) {
        uint8_t version = request->bytes[sizeof(request->bytes)];
        fetchDatatype = (version == uint8_t(GetMetaVersion::V2));
    }
}

ENGINE_ERROR_CODE GetMetaCommandContext::getItemMeta() {
    auto errorMetaPair = bucket_get_meta(&connection, key, vbucket);
    if (errorMetaPair.first == cb::engine_errc::success) {
        info = errorMetaPair.second;
        state = State::SendResponse;
    } else if (errorMetaPair.first == cb::engine_errc::no_such_key) {
        state = State::NoSuchItem;
        errorMetaPair.first = cb::engine_errc::success;
    }

    return ENGINE_ERROR_CODE(errorMetaPair.first);
}

ENGINE_ERROR_CODE GetMetaCommandContext::sendResponse() {
    // Prepare response
    uint32_t deleted =
            htonl(info.document_state == DocumentState::Deleted ? 1 : 0);
    uint8_t datatype = fetchDatatype ? info.datatype : 0;
    GetMetaResponse metaResponse(deleted,
                                 info.flags,
                                 htonl(info.exptime),
                                 htonll(info.seqno),
                                 datatype);

    const uint8_t responseExtlen =
            fetchDatatype
                    ? sizeof(metaResponse)
                    : sizeof(metaResponse) - sizeof(metaResponse.datatype);

    // Send response
    mcbp_response_handler(nullptr /*key*/,
                          0 /*keylen*/,
                          reinterpret_cast<uint8_t*>(&metaResponse),
                          responseExtlen,
                          nullptr /*body*/,
                          0 /*bodylen*/,
                          PROTOCOL_BINARY_RAW_BYTES,
                          PROTOCOL_BINARY_RESPONSE_SUCCESS,
                          info.cas,
                          connection.getCookie());

    mcbp_write_and_free(&connection, &connection.getDynamicBuffer());

    state = State::Done;
    return ENGINE_SUCCESS;
}

ENGINE_ERROR_CODE GetMetaCommandContext::noSuchItem() {
    if (connection.isNoReply()) {
        auto& bucket = connection.getBucket();
        bucket.responseCounters[PROTOCOL_BINARY_RESPONSE_KEY_ENOENT]++;
        connection.setState(conn_new_cmd);
    } else {
        if (request->message.header.request.opcode !=
            PROTOCOL_BINARY_CMD_GETQ_META) {
            mcbp_write_packet(&connection, PROTOCOL_BINARY_RESPONSE_KEY_ENOENT);
        }
    }

    state = State::Done;
    return ENGINE_SUCCESS;
}

ENGINE_ERROR_CODE GetMetaCommandContext::step() {
    ENGINE_ERROR_CODE ret;
    do {
        switch (state) {
        case State::GetItemMeta:
            ret = getItemMeta();
            break;
        case State::NoSuchItem:
            ret = noSuchItem();
            break;
        case State::SendResponse:
            ret = sendResponse();
            break;
        case State::Done:
            return ENGINE_SUCCESS;
        }
    } while (ret == ENGINE_SUCCESS);

    return ret;
}
