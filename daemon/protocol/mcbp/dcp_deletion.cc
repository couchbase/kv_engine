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
#include <mcbp/protocol/header.h>
#include <mcbp/protocol/request.h>
#include <memcached/limits.h>
#include <memcached/protocol_binary.h>

static ENGINE_ERROR_CODE dcp_deletion_v1_executor(Cookie& cookie) {
    auto& request = cookie.getHeader().getRequest();
    const auto key = cookie.getConnection().makeDocKey(request.getKey());
    const auto opaque = request.getOpaque();
    const auto datatype = uint8_t(request.getDatatype());
    const auto cas = request.getCas();
    const Vbid vbucket = request.getVBucket();

    auto extras = request.getExtdata();
    using cb::mcbp::request::DcpDeletionV1Payload;

    if (extras.size() != sizeof(DcpDeletionV1Payload)) {
        return ENGINE_EINVAL;
    }
    auto* payload =
            reinterpret_cast<const DcpDeletionV1Payload*>(extras.data());

    const uint64_t by_seqno = payload->getBySeqno();
    const uint64_t rev_seqno = payload->getRevSeqno();
    const uint16_t nmeta = payload->getNmeta();

    auto value = request.getValue();
    cb::const_byte_buffer meta{value.data() + value.size() - nmeta, nmeta};
    value = {value.data(), value.size() - nmeta};

    uint32_t priv_bytes = 0;
    if (mcbp::datatype::is_xattr(datatype)) {
        priv_bytes = gsl::narrow<uint32_t>(value.size());
    }
    if (priv_bytes <= cb::limits::PrivilegedBytes) {
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
    }
    return ENGINE_E2BIG;
}

// The updated deletion sends no extended meta, but does send a deletion time
// and the collection_len
static ENGINE_ERROR_CODE dcp_deletion_v2_executor(Cookie& cookie) {
    auto& request = cookie.getHeader().getRequest();

    auto& connection = cookie.getConnection();
    const auto key = connection.makeDocKey(request.getKey());

    const auto opaque = request.getOpaque();
    const auto datatype = uint8_t(request.getDatatype());
    const auto cas = request.getCas();
    const Vbid vbucket = request.getVBucket();

    auto extras = request.getExtdata();
    using cb::mcbp::request::DcpDeletionV2Payload;

    if (extras.size() != sizeof(DcpDeletionV2Payload)) {
        return ENGINE_EINVAL;
    }
    auto* payload =
            reinterpret_cast<const DcpDeletionV2Payload*>(extras.data());

    const uint64_t by_seqno = payload->getBySeqno();
    const uint64_t rev_seqno = payload->getRevSeqno();
    const uint32_t delete_time = payload->getDeleteTime();

    auto value = request.getValue();
    uint32_t priv_bytes = 0;
    if (mcbp::datatype::is_xattr(datatype)) {
        priv_bytes = gsl::narrow<uint32_t>(value.size());
    }

    if (priv_bytes <= cb::limits::PrivilegedBytes) {
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
    }
    return ENGINE_E2BIG;
}

void dcp_deletion_executor(Cookie& cookie) {
    auto& connection = cookie.getConnection();

    auto ret = cookie.swapAiostat(ENGINE_SUCCESS);

    if (ret == ENGINE_SUCCESS) {
        if (connection.isDcpDeleteV2()) {
            ret = dcp_deletion_v2_executor(cookie);
        } else {
            ret = dcp_deletion_v1_executor(cookie);
        }
    }

    ret = connection.remapErrorCode(ret);
    switch (ret) {
    case ENGINE_SUCCESS:
        connection.setState(StateMachine::State::new_cmd);
        break;

    case ENGINE_DISCONNECT:
        connection.shutdown();
        break;

    case ENGINE_EWOULDBLOCK:
        cookie.setEwouldblock(true);
        break;

    default:
        cookie.sendResponse(cb::engine_errc(ret));
    }
}
