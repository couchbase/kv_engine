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

#include "executors.h"

#include <daemon/mcaudit.h>
#include <daemon/mcbp.h>
#include <string>
#include "engine_wrapper.h"
#include "utilities.h"

void dcp_open_executor(Cookie& cookie) {
    auto packet = cookie.getPacket(Cookie::PacketContent::Full);
    const auto* req = reinterpret_cast<const protocol_binary_request_dcp_open*>(
            packet.data());

    auto ret = cookie.swapAiostat(ENGINE_SUCCESS);

    auto& connection = cookie.getConnection();
    connection.enableDatatype(cb::mcbp::Feature::JSON);

    uint32_t flags = ntohl(req->message.body.flags);
    const bool dcpNotifier = (flags & DCP_OPEN_NOTIFIER) == DCP_OPEN_NOTIFIER;

    if (ret == ENGINE_SUCCESS) {
        cb::rbac::Privilege privilege = cb::rbac::Privilege::DcpProducer;
        if (dcpNotifier) {
            privilege = cb::rbac::Privilege::DcpConsumer;
        }

        ret = mcbp::checkPrivilege(cookie, privilege);

        const uint16_t nkey = ntohs(req->message.header.request.keylen);
        const uint32_t valuelen = ntohl(req->message.header.request.bodylen) -
                                  nkey - req->message.header.request.extlen;

        const auto* name =
                reinterpret_cast<const char*>(req->bytes + sizeof(req->bytes));

        auto dcpOpenFunc = [&]() -> ENGINE_ERROR_CODE {
            boost::optional<cb::const_char_buffer> collections;
            // Initialise the optional only if collections is enabled and even
            // if valuelen is 0
            if (cookie.getConnection().isCollectionsSupported()) {
                collections = {reinterpret_cast<const char*>(
                                       req->bytes + sizeof(req->bytes) + nkey),
                               valuelen};
            }
            return dcpOpen(cookie,
                           req->message.header.request.opaque,
                           ntohl(req->message.body.seqno),
                           flags,
                           {name, nkey},
                           collections);
        };

        // Collections Prototype: The following code allows the bucket to decide
        // if this stream should be forced to being collection aware. So we can
        // run with collections enabled, but with a non-collection bucket and a
        // collection bucket. This is only whilst collections are in development
        if (ret == ENGINE_SUCCESS) {
            ret = dcpOpenFunc();
            if (settings.isCollectionsPrototypeEnabled() &&
                ret == ENGINE_UNKNOWN_COLLECTION) {
                // Force collections on
                cookie.getConnection().setCollectionsSupported(true);
                ret = dcpOpenFunc();
                LOG_INFO("{}: Retried DCP open with collections enabled ret:{}",
                         connection.getId(),
                         ret);
            }
        }
    }

    ret = connection.remapErrorCode(ret);
    switch (ret) {
    case ENGINE_SUCCESS: {
        const bool dcpXattrAware = (flags & DCP_OPEN_INCLUDE_XATTRS) != 0 &&
                                   connection.selectedBucketIsXattrEnabled();
        const bool dcpNoValue = (flags & DCP_OPEN_NO_VALUE) != 0;
        const bool dcpDeleteTimes =
                (flags & DCP_OPEN_INCLUDE_DELETE_TIMES) != 0;
        connection.setDcpXattrAware(dcpXattrAware);
        connection.setDcpNoValue(dcpNoValue);
        connection.setDcpDeleteTimeEnabled(dcpDeleteTimes);

        // String buffer with max length = total length of all possible contents
        std::string logBuffer;

        const bool dcpProducer =
                (flags & DCP_OPEN_PRODUCER) == DCP_OPEN_PRODUCER;
        if (dcpProducer) {
            logBuffer.append("PRODUCER, ");
        }
        if (dcpNotifier) {
            logBuffer.append("NOTIFIER, ");
        }
        if (dcpXattrAware) {
            logBuffer.append("INCLUDE_XATTRS, ");
        }
        if (dcpNoValue) {
            logBuffer.append("NO_VALUE, ");
        }
        if (dcpDeleteTimes) {
            logBuffer.append("DELETE_TIMES, ");
        }

        // Remove trailing whitespace and comma
        if (!logBuffer.empty()) {
            logBuffer.resize(logBuffer.size() - 2);
        }

        LOG_INFO("{}: DCP connection opened successfully. {} {}",
                 connection.getId(),
                 logBuffer,
                 connection.getDescription().c_str());

        audit_dcp_open(&connection);
        cookie.sendResponse(cb::mcbp::Status::Success);
        break;
    }

    case ENGINE_DISCONNECT:
        connection.setState(McbpStateMachine::State::closing);
        break;

    case ENGINE_EWOULDBLOCK:
        cookie.setEwouldblock(true);
        break;

    default:
        cookie.sendResponse(cb::engine_errc(ret));
    }
}
