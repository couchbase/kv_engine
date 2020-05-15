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

#include "executors.h"

#include "engine_wrapper.h"
#include "utilities.h"

void dcp_noop_executor(Cookie& cookie) {
    auto ret = cookie.swapAiostat(ENGINE_SUCCESS);

    if (ret == ENGINE_SUCCESS) {
        // NOOP may be sent to a consumer or a producer...
        ret = mcbp::haveDcpPrivilege(cookie);
        if (ret == ENGINE_SUCCESS) {
            const auto& header = cookie.getHeader();
            ret = dcpNoop(cookie, header.getOpaque());
        }
    }

    handle_executor_status(cookie, ret);
}
