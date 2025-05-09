/*
 *     Copyright 2025-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "get_vbucket_command_context.h"
#include "engine_wrapper.h"

GetVbucketCommandContext::GetVbucketCommandContext(Cookie& cookie)
    : SteppableCommandContext(cookie) {
}

cb::engine_errc GetVbucketCommandContext::step() {
    const auto [status, state] = bucket_get_vbucket(cookie);
    if (status == cb::engine_errc::success) {
        uint32_t st = ntohl(uint32_t(state));
        cookie.sendResponse(cb::mcbp::Status::Success,
                            {},
                            {},
                            {reinterpret_cast<const char*>(&st), sizeof(st)},
                            cb::mcbp::Datatype::Raw,
                            cb::mcbp::cas::Wildcard);
    }
    return status;
}