/*
 *     Copyright 2024-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "release_snapshot_context.h"

#include <daemon/buckets.h>
#include <daemon/concurrency_semaphores.h>
#include <daemon/connection.h>
#include <logger/logger.h>
#include <memcached/engine.h>

ReleaseSnapshotContext::ReleaseSnapshotContext(Cookie& cookie)
    : BackgroundThreadCommandContext(
              cookie,
              TaskId::Core_ReleaseSnapshotTask,
              "Release Snapshot",
              ConcurrencySemaphores::instance()
                      .encryption_and_snapshot_management),
      uuid(cookie.getRequest().getKeyString()),
      vbid(cookie.getRequest().getVBucket()) {
}

cb::engine_errc ReleaseSnapshotContext::execute() {
    try {
        // release by vbid or uuid?
        if (uuid.empty()) {
            return connection.getBucket().getEngine().release_snapshot(cookie,
                                                                       vbid);
        }

        return connection.getBucket().getEngine().release_snapshot(cookie,
                                                                   uuid);

    } catch (const std::exception& e) {
        if (uuid.empty()) {
            LOG_WARNING_CTX("Exception occurred while releasing snapshot",
                            {"conn_id", connection.getId()},
                            {"vb", vbid},
                            {"error", e.what()});
        } else {
            LOG_WARNING_CTX("Exception occurred while releasing snapshot",
                            {"conn_id", connection.getId()},
                            {"uuid", uuid},
                            {"error", e.what()});
        }
        response = fmt::format("Failed: {}", e.what());
    }

    return cb::engine_errc::failed;
}
