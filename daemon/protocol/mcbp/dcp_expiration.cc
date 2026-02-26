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
#include "no_success_response_steppable_context.h"

#include <memcached/protocol_binary.h>
#include <xattr/blob.h>

static cb::engine_errc dcp_expiration(Cookie& cookie) {
    const auto& connection = cookie.getConnection();
    const auto& req = cookie.getRequest();
    using cb::mcbp::request::DcpExpirationPayload;
    const auto& extras = req.getCommandSpecifics<DcpExpirationPayload>();
    return dcpExpiration(cookie,
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

void dcp_expiration_executor(Cookie& cookie) {
    cookie.obtainContext<NoSuccessResponseCommandContext>(
                  cookie, [](Cookie& c) { return dcp_expiration(c); })
            .drive();
}