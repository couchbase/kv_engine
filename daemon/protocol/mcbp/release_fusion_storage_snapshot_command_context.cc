/*
 *     Copyright 2025-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "release_fusion_storage_snapshot_command_context.h"

#include <daemon/buckets.h>
#include <daemon/concurrency_semaphores.h>
#include <daemon/connection.h>
#include <memcached/engine.h>

ReleaseFusionStorageSnapshotCommandContext::
        ReleaseFusionStorageSnapshotCommandContext(Cookie& cookie)
    : BackgroundThreadCommandContext(
              cookie,
              TaskId::Core_ReleaseFusionStorageSnapshotTask,
              fmt::format("Core_ReleaseFusionStorageSnapshotTask:{}",
                          cookie.getRequest().getVBucket()),
              ConcurrencySemaphores::instance().fusion_management) {
}

cb::engine_errc ReleaseFusionStorageSnapshotCommandContext::execute() {
    const auto& req = cookie.getRequest();
    const auto request = nlohmann::json::parse(req.getValueString());
    const std::string snapshotUuid = request["snapshotUuid"];
    auto& engine = cookie.getConnection().getBucketEngine();
    return engine.releaseFusionStorageSnapshot(req.getVBucket(), snapshotUuid);
}
