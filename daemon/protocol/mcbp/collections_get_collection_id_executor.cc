/*
 *     Copyright 2018-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "executors.h"

#include <daemon/cookie.h>
#include <memcached/collections.h>

void collections_get_collection_id_executor(Cookie& cookie) {
    auto& connection = cookie.getConnection();
    auto& req = cookie.getRequest();
    std::string_view path = req.getKeyString();
    if (path.empty()) {
        path = req.getValueString();
    }
    auto rv = connection.getBucketEngine().get_collection_id(cookie, path);
    if (rv.result == cb::engine_errc::success) {
        auto payload = rv.getPayload();
        cookie.sendResponse(cb::mcbp::Status::Success,
                            payload.getBuffer(),
                            {},
                            {},
                            cb::mcbp::Datatype::Raw,
                            0);
    } else {
        Expects(rv.result != cb::engine_errc::would_block);
        handle_executor_status(cookie, rv.result);
    }
}
