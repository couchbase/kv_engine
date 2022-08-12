/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2017-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#include "dcp_deletion.h"
#include "engine_wrapper.h"
#include "executors.h"
#include "utilities.h"
#include <mcbp/protocol/header.h>
#include <mcbp/protocol/request.h>
#include <memcached/limits.h>
#include <memcached/protocol_binary.h>

static cb::engine_errc dcp_deletion_v1_executor(Cookie& cookie) {
    auto& request = cookie.getHeader().getRequest();
    const auto key = cookie.getConnection().makeDocKey(request.getKey());
    const auto opaque = request.getOpaque();
    const auto datatype = uint8_t(request.getDatatype());
    const auto cas = request.getCas();
    const Vbid vbucket = request.getVBucket();

    auto extras = request.getExtdata();
    using cb::mcbp::request::DcpDeletionV1Payload;

    if (extras.size() != sizeof(DcpDeletionV1Payload)) {
        return cb::engine_errc::invalid_arguments;
    }

    const auto& payload = request.getCommandSpecifics<DcpDeletionV1Payload>();
    const uint64_t by_seqno = payload.getBySeqno();
    const uint64_t rev_seqno = payload.getRevSeqno();
    const uint16_t nmeta = payload.getNmeta();

    auto value = request.getValue();
    cb::const_byte_buffer meta{value.data() + value.size() - nmeta, nmeta};
    value = {value.data(), value.size() - nmeta};

    uint32_t priv_bytes = 0;
    if (cb::mcbp::datatype::is_xattr(datatype)) {
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
    return cb::engine_errc::too_big;
}

// The updated deletion sends no extended meta, but does send a deletion time
// and the collection_len
static cb::engine_errc dcp_deletion_v2_executor(Cookie& cookie) {
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
        return cb::engine_errc::invalid_arguments;
    }
    const auto& payload = request.getCommandSpecifics<DcpDeletionV2Payload>();
    const uint64_t by_seqno = payload.getBySeqno();
    const uint64_t rev_seqno = payload.getRevSeqno();
    const uint32_t delete_time = payload.getDeleteTime();

    auto value = request.getValue();
    uint32_t priv_bytes = 0;
    if (cb::mcbp::datatype::is_xattr(datatype)) {
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
    return cb::engine_errc::too_big;
}

void dcp_deletion_executor(Cookie& cookie) {
    auto& connection = cookie.getConnection();

    auto ret = cookie.swapAiostat(cb::engine_errc::success);

    if (ret == cb::engine_errc::success) {
        if (connection.isDcpDeleteV2()) {
            ret = dcp_deletion_v2_executor(cookie);
        } else {
            ret = dcp_deletion_v1_executor(cookie);
        }
    }

    if (ret != cb::engine_errc::success) {
        handle_executor_status(cookie, ret);
    }
}
