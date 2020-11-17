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
#include "dcp_add_failover_log.h"
#include <daemon/cookie.h>
#include <daemon/mcbp.h>
#include <mcbp/protocol/datatype.h>

/** Callback from the engine adding the response */
ENGINE_ERROR_CODE add_failover_log(std::vector<vbucket_failover_t> entries,
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
    return ENGINE_SUCCESS;
}
