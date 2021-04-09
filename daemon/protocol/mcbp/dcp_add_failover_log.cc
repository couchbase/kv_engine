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
#include "dcp_add_failover_log.h"
#include <daemon/cookie.h>
#include <daemon/mcbp.h>
#include <mcbp/protocol/datatype.h>

/** Callback from the engine adding the response */
cb::engine_errc add_failover_log(std::vector<vbucket_failover_t> entries,
                                 Cookie& cookie) {
    for (auto& entry : entries) {
        entry.uuid = htonll(entry.uuid);
        entry.seqno = htonll(entry.seqno);
    }
    cookie.sendResponse(
            cb::mcbp::Status::Success,
            {},
            {},
            {reinterpret_cast<const char*>(entries.data()),
             (uint32_t)(entries.size() * sizeof(vbucket_failover_t))},
            cb::mcbp::Datatype::Raw,
            0);
    return cb::engine_errc::success;
}
