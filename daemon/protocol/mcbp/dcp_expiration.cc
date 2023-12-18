/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2018-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#include "dcp_expiration.h"
#include "engine_wrapper.h"
#include "executors.h"
#include "utilities.h"
#include <memcached/protocol_binary.h>
#include <xattr/blob.h>

void dcp_expiration_executor(Cookie& cookie) {
    auto ret = cookie.swapAiostat(cb::engine_errc::success);

    auto& connection = cookie.getConnection();
    if (ret == cb::engine_errc::success) {
        const auto& req = cookie.getRequest();
        using cb::mcbp::request::DcpExpirationPayload;
        const auto& extras = req.getCommandSpecifics<DcpExpirationPayload>();
        ret = dcpExpiration(cookie,
                            req.getOpaque(),
                            connection.makeDocKey(req.getKey()),
                            req.getValue(),
                            uint8_t(req.getDatatype()),
                            req.getCas(),
                            req.getVBucket(),
                            extras.getBySeqno(),
                            extras.getRevSeqno(),
                            extras.getDeleteTime());
    }

    if (ret != cb::engine_errc::success) {
        handle_executor_status(cookie, ret);
    }
}
