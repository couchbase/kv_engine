/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2016 Couchbase, Inc.
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
#include "engine_wrapper.h"
#include "gat_context.h"

#include <daemon/debug_helpers.h>
#include <daemon/mcbp.h>
#include <xattr/utils.h>
#include <daemon/mcaudit.h>

GatCommandContext::GatCommandContext(McbpConnection& c,
                                     const protocol_binary_request_gat& req)
    : SteppableCommandContext(c),
      key(req.bytes + sizeof(req.bytes),
          ntohs(req.message.header.request.keylen),
          c.getDocNamespace()),
      vbucket(ntohs(req.message.header.request.vbucket)),
      exptime(ntohl(req.message.body.expiration)),
      it(nullptr, cb::ItemDeleter{c.getBucketEngineAsV0()}),
      info{},
      state(State::GetAndTouchItem) {
}

ENGINE_ERROR_CODE GatCommandContext::getAndTouchItem() {
    auto ret = bucket_get_and_touch(&connection, key, vbucket, exptime);
    if (ret.first == cb::engine_errc::success) {
        it = std::move(ret.second);
        if (!bucket_get_item_info(&connection, it.get(), &info)) {
            LOG_WARNING(&connection, "%u: Failed to get item info",
                        connection.getId());
            return ENGINE_FAILED;
        }

        // Set the cas value (will be copied to the response message)
        connection.setCAS(info.cas);
        payload.buf = static_cast<const char*>(info.value[0].iov_base);
        payload.len = info.value[0].iov_len;

        bool need_inflate = false;
        if (mcbp::datatype::is_snappy(info.datatype) &&
            connection.getCmd() != PROTOCOL_BINARY_CMD_TOUCH) {
            need_inflate = mcbp::datatype::is_xattr(info.datatype) ||
                           !connection.isSnappyEnabled();
        }

        if (need_inflate) {
            state = State::InflateItem;
        } else {
            state = State::SendResponse;
        }
    } else if (ret.first == cb::engine_errc::no_such_key) {
        state = State::NoSuchItem;
        ret.first = cb::engine_errc::success;
    }

    return ENGINE_ERROR_CODE(ret.first);
}

ENGINE_ERROR_CODE GatCommandContext::inflateItem() {
    try {
        if (!cb::compression::inflate(cb::compression::Algorithm::Snappy,
                                      payload.buf, payload.len, buffer)) {
            LOG_WARNING(&connection, "%u: Failed to inflate item",
                        connection.getId());
            return ENGINE_FAILED;
        }
        payload.buf = buffer.data.get();
        payload.len = buffer.len;
    } catch (const std::bad_alloc&) {
        return ENGINE_ENOMEM;
    }

    state = State::SendResponse;
    return ENGINE_SUCCESS;
}

ENGINE_ERROR_CODE GatCommandContext::sendResponse() {
    STATS_HIT(&connection, get);
    update_topkeys(key, &connection);

    // Audit the modification to the document (change of EXP)
    cb::audit::document::add(connection,
                             cb::audit::document::Operation::Modify);

    if (connection.getCmd() == PROTOCOL_BINARY_CMD_TOUCH) {
        mcbp_write_packet(&connection, PROTOCOL_BINARY_RESPONSE_SUCCESS);
        state = State::Done;
        return ENGINE_SUCCESS;
    }

    // This is GAT[Q]
    protocol_binary_datatype_t datatype = info.datatype;
    if (mcbp::datatype::is_xattr(datatype)) {
        payload = cb::xattr::get_body(payload);
        datatype &= ~PROTOCOL_BINARY_DATATYPE_XATTR;
    }
    datatype = connection.getEnabledDatatypes(datatype);

    uint32_t bodylen = sizeof(info.flags) + payload.len;
    // Set the CAS to add into the header
    connection.setCAS(info.cas);
    mcbp_add_header(&connection,
                    PROTOCOL_BINARY_RESPONSE_SUCCESS,
                    sizeof(info.flags),
                    0, // no key
                    bodylen,
                    datatype);

    // Add the flags
    connection.addIov(&info.flags, sizeof(info.flags));
    // Add the value
    connection.addIov(payload.buf, payload.len);
    connection.setState(McbpStateMachine::State::send_data);
    cb::audit::document::add(connection, cb::audit::document::Operation::Read);
    state = State::Done;
    return ENGINE_SUCCESS;
}

ENGINE_ERROR_CODE GatCommandContext::noSuchItem() {
    STATS_MISS(&connection, get);
    if (connection.isNoReply()) {
        ++connection.getBucket()
                    .responseCounters[PROTOCOL_BINARY_RESPONSE_KEY_ENOENT];
        connection.setState(McbpStateMachine::State::new_cmd);
    } else {
        mcbp_write_packet(&connection, PROTOCOL_BINARY_RESPONSE_KEY_ENOENT);
    }

    state = State::Done;
    return ENGINE_SUCCESS;
}

ENGINE_ERROR_CODE GatCommandContext::step() {
    ENGINE_ERROR_CODE ret;
    do {
        switch (state) {
        case State::GetAndTouchItem:
            ret = getAndTouchItem();
            break;
        case State::NoSuchItem:
            ret = noSuchItem();
            break;
        case State::InflateItem:
            ret = inflateItem();
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
