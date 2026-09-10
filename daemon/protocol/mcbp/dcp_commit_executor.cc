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

static cb::engine_errc do_dcp_commit(Cookie& cookie) {
    const auto& req = cookie.getRequest();
    using cb::mcbp::request::DcpCommitPayload;
    const auto& extras = req.getCommandSpecifics<DcpCommitPayload>();
    return dcpCommit(cookie,
                     req.getOpaque(),
                     req.getVBucket(),
                     cookie.getConnection().makeDocKey(req.getKey()),
                     extras.getPreparedSeqno(),
                     extras.getCommitSeqno());
}

void dcp_commit_executor(Cookie& cookie) {
    cookie.obtainContext<NoSuccessResponseCommandContext>(
                  cookie, [](Cookie& c) { return do_dcp_commit(c); })
            .drive();
}
