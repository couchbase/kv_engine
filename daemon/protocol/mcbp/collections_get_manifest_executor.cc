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

#include <daemon/buckets.h>
#include <daemon/cookie.h>
#include <daemon/mcbp.h>
#include <memcached/engine.h>

void collections_get_manifest_executor(Cookie& cookie) {
    auto& connection = cookie.getConnection();
    const auto ret = connection.getBucketEngine().get_collection_manifest(
            &cookie, mcbpResponseHandlerFn);
    if (ret != cb::engine_errc::success) {
        Expects(ret != cb::engine_errc::would_block);
        handle_executor_status(cookie, ret);
    }
}
