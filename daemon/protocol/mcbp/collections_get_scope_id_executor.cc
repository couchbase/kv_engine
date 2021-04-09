/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2019-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "executors.h"

#include <daemon/cookie.h>
#include <mcbp/protocol/request.h>

void collections_get_scope_id_executor(Cookie& cookie) {
    auto& connection = cookie.getConnection();
    auto& req = cookie.getRequest();
    auto key = req.getKey();
    std::string_view path{};

    if (key.size()) {
        path = std::string_view{reinterpret_cast<const char*>(key.data()),
                                key.size()};
    } else {
        auto value = req.getValue();
        path = std::string_view{reinterpret_cast<const char*>(value.data()),
                                value.size()};
    }
    auto rv = connection.getBucketEngine().get_scope_id(&cookie, path);
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
