/*
 *     Copyright 2018-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#include "engine_wrapper.h"
#include "executors.h"
#include "no_success_response_steppable_context.h"
#include <daemon/cookie.h>
#include <memcached/protocol_binary.h>

static cb::engine_errc do_dcp_abort(Cookie& cookie) {
    const auto& req = cookie.getRequest();
    using cb::mcbp::request::DcpAbortPayload;
    const auto& extras = req.getCommandSpecifics<DcpAbortPayload>();
    return dcpAbort(cookie,
                    req.getOpaque(),
                    req.getVBucket(),
                    cookie.getConnection().makeDocKey(req.getKey()),
                    extras.getPreparedSeqno(),
                    extras.getAbortSeqno());
}

void dcp_abort_executor(Cookie& cookie) {
    cookie.obtainContext<NoSuccessResponseCommandContext>(
                  cookie, [](Cookie& c) { return do_dcp_abort(c); })
            .drive();
}
