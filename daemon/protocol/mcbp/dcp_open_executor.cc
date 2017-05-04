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

#include <daemon/mcaudit.h>
#include <daemon/mcbp.h>
#include "executors.h"
#include "utilities.h"

void dcp_open_executor(McbpConnection* c, void* packet) {
    auto* req = reinterpret_cast<protocol_binary_request_dcp_open*>(packet);

    ENGINE_ERROR_CODE ret = c->getAiostat();
    c->setAiostat(ENGINE_SUCCESS);
    c->setEwouldblock(false);
    c->enableDatatype(mcbp::Feature::SNAPPY);
    c->enableDatatype(mcbp::Feature::JSON);

    uint32_t flags = ntohl(req->message.body.flags);
    const bool dcpNotifier = (flags & DCP_OPEN_NOTIFIER);

    if (ret == ENGINE_SUCCESS) {
        cb::rbac::Privilege privilege = cb::rbac::Privilege::DcpProducer;
        if (dcpNotifier) {
            privilege = cb::rbac::Privilege::DcpConsumer;
        }

        ret = mcbp::checkPrivilege(*c, privilege);
        if (ret == ENGINE_SUCCESS) {
            ret = c->getBucketEngine()->dcp.open(c->getBucketEngineAsV0(),
                                                 c->getCookie(),
                                                 req->message.header.request.opaque,
                                                 ntohl(req->message.body.seqno),
                                                 flags,
                                                 (void*)(req->bytes +
                                                         sizeof(req->bytes)),
                                                 ntohs(
                                                     req->message.header.request.keylen));
        }
    }

    ret = c->remapErrorCode(ret);
    switch (ret) {
    case ENGINE_SUCCESS: {
        const bool dcpXattrAware = (flags & DCP_OPEN_INCLUDE_XATTRS) != 0;
        const bool dcpNoValue = (flags & DCP_OPEN_NO_VALUE) != 0;
        c->setDcpXattrAware(dcpXattrAware);
        c->setDcpNoValue(dcpNoValue);

        // @todo Keeping this as NOTICE while waiting for ns_server
        //       support for xattr over DCP (to make it easier to debug
        ///      see MB-22468
        LOG_NOTICE(c,
                   "%u: DCP connection opened successfully. flags:{%s%s%s} %s",
                   c->getId(),
                   dcpNotifier ? "NOTIFIER " : "",
                   dcpXattrAware ? "INCLUDE_XATTRS " : "",
                   dcpNoValue ? "NO_VALUE " : "",
                   c->getDescription().c_str());

        audit_dcp_open(c);
        mcbp_write_packet(c, PROTOCOL_BINARY_RESPONSE_SUCCESS);
        break;
    }

    case ENGINE_DISCONNECT:
        c->setState(conn_closing);
        break;

    case ENGINE_EWOULDBLOCK:
        c->setEwouldblock(true);
        break;

    default:
        mcbp_write_packet(c, engine_error_2_mcbp_protocol_error(ret));
    }
}
