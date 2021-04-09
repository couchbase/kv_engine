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

#include "executors.h"

#include <daemon/cookie.h>
#include <memcached/engine.h>
#include <memcached/protocol_binary.h>

void collections_set_manifest_executor(Cookie& cookie) {
    auto& connection = cookie.getConnection();
    auto ret = cookie.swapAiostat(cb::engine_errc::success);
    if (ret == cb::engine_errc::success) {
        auto& req = cookie.getRequest();
        auto val = req.getValue();
        std::string_view jsonBuffer{reinterpret_cast<const char*>(val.data()),
                                    val.size()};
        auto status = connection.getBucketEngine().set_collection_manifest(
                &cookie, jsonBuffer);
        handle_executor_status(cookie, status);
    } else {
        handle_executor_status(cookie, ret);
    }
}