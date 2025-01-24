/*
 *     Copyright 2025-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "get_fusion_storage_snapshot_command_context.h"

#include <daemon/buckets.h>
#include <daemon/concurrency_semaphores.h>
#include <daemon/connection.h>
#include <memcached/engine.h>

GetFusionStorageSnapshotCommandContext::GetFusionStorageSnapshotCommandContext(
        Cookie& cookie)
    : BackgroundThreadCommandContext(
              cookie,
              TaskId::Core_GetFusionStorageSnapshotTask,
              fmt::format("Core_GetFusionStorageSnapshotTask:{}",
                          cookie.getRequest().getVBucket()),
              ConcurrencySemaphores::instance().fusion_management) {
}

cb::engine_errc GetFusionStorageSnapshotCommandContext::execute() {
    const auto& req = cookie.getRequest();
    const auto request = nlohmann::json::parse(req.getValueString());
    const std::string snapshotUuid = request["snapshotUuid"];
    const std::time_t validity = request["validity"];

    auto& engine = cookie.getConnection().getBucketEngine();
    auto [ec, json] = engine.getFusionStorageSnapshot(
            req.getVBucket(), snapshotUuid, validity);
    if (ec == cb::engine_errc::success) {
        response = json.dump();
        datatype = cb::mcbp::Datatype::JSON;
    }
    return ec;
}
