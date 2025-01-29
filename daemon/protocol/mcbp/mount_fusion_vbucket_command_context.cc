/*
 *     Copyright 2025-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "mount_fusion_vbucket_command_context.h"

#include <daemon/concurrency_semaphores.h>
#include <daemon/connection.h>
#include <executor/globaltask.h>

MountFusionVbucketCommandContext::MountFusionVbucketCommandContext(
        Cookie& cookie)
    : BackgroundThreadCommandContext(
              cookie,
              TaskId::Core_MountFusionVBucketTask,
              fmt::format("MountFusionVbucket:{}",
                          cookie.getRequest().getVBucket()),
              ConcurrencySemaphores::instance().fusion_management) {
}

cb::engine_errc MountFusionVbucketCommandContext::execute() {
    const auto& req = cookie.getRequest();
    auto& engine = cookie.getConnection().getBucketEngine();
    const auto request = nlohmann::json::parse(req.getValueString());
    std::vector<std::string> paths = request["mountPaths"];

    const auto [ec, deks] = engine.mountVBucket(req.getVBucket(), paths);
    if (ec == cb::engine_errc::success) {
        nlohmann::json json;
        json["deks"] = deks;
        response = json.dump();
        datatype = cb::mcbp::Datatype::JSON;
    }
    return ec;
}
