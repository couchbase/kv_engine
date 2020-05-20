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

#include <daemon/buckets.h>
#include <daemon/cookie.h>
#include <daemon/mcbp.h>
#include <logger/logger.h>
#include <memcached/engine.h>

void collections_get_manifest_executor(Cookie& cookie) {
    auto& connection = cookie.getConnection();
    auto ret = connection.getBucketEngine()->get_collection_manifest(
            &cookie, mcbpResponseHandlerFn);

    const auto mapped = cookie.getConnection().remapErrorCode(ret);
    switch (mapped) {
    case cb::engine_errc::success: {
        if (cookie.getDynamicBuffer().getRoot() != nullptr) {
            // We assume that if the underlying engine returns a success then
            // it is sending a success to the client.
            ++connection.getBucket()
                      .responseCounters[int(cb::mcbp::Status::Success)];
            cookie.sendDynamicBuffer();
        } else {
            connection.setState(StateMachine::State::new_cmd);
        }
        break;
    }
    case cb::engine_errc::disconnect:
        if (ret == cb::engine_errc::disconnect) {
            connection.setTerminationReason("Engine forced disconnect");
        }
        connection.setState(StateMachine::State::closing);
        break;
    default:
        // Release the dynamic buffer.. it may be partial..
        cookie.clearDynamicBuffer();
        cookie.sendResponse(cb::engine_errc(mapped));
    }
}
