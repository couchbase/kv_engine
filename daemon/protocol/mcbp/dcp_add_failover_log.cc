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
#include "config.h"

#include <daemon/mcbp.h>
#include "dcp_add_failover_log.h"

/** Callback from the engine adding the response */
ENGINE_ERROR_CODE add_failover_log(vbucket_failover_t* entries,
                                   size_t nentries,
                                   const void* cookie) {
    ENGINE_ERROR_CODE ret;
    size_t ii;
    for (ii = 0; ii < nentries; ++ii) {
        entries[ii].uuid = htonll(entries[ii].uuid);
        entries[ii].seqno = htonll(entries[ii].seqno);
    }

    if (mcbp_response_handler(NULL, 0, NULL, 0, entries,
                              (uint32_t)(nentries * sizeof(vbucket_failover_t)),
                              PROTOCOL_BINARY_RAW_BYTES,
                              PROTOCOL_BINARY_RESPONSE_SUCCESS, 0,
                              (void*)cookie)) {
        ret = ENGINE_SUCCESS;
    } else {
        ret = ENGINE_ENOMEM;
    }

    for (ii = 0; ii < nentries; ++ii) {
        entries[ii].uuid = htonll(entries[ii].uuid);
        entries[ii].seqno = htonll(entries[ii].seqno);
    }

    return ret;
}