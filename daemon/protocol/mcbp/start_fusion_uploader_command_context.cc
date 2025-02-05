/*
 *     Copyright 2025-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#include "start_fusion_uploader_command_context.h"

#include <daemon/buckets.h>
#include <daemon/concurrency_semaphores.h>
#include <daemon/connection.h>
#include <logger/logger.h>
#include <memcached/engine.h>

StartFusionUploaderCommandContext::StartFusionUploaderCommandContext(
        Cookie& cookie)
    : BackgroundThreadCommandContext(
              cookie,
              TaskId::Core_StartFusionUploaderTask,
              fmt::format("StartFusionUploader {}",
                          cookie.getRequest().getVBucket()),
              ConcurrencySemaphores::instance().fusion_management) {
}

cb::engine_errc StartFusionUploaderCommandContext::execute() {
    try {
        const auto& req = cookie.getRequest();
        const auto args = nlohmann::json::parse(req.getValueString());
        const std::string term = args["term"];
        auto& engine = cookie.getConnection().getBucketEngine();
        return engine.startFusionUploader(req.getVBucket(), std::stoull(term));
    } catch (const std::exception& e) {
        LOG_WARNING_CTX("StartFusionUploaderCommandContext: ",
                        {"error", e.what()});
        response = fmt::format("Failed: {}", e.what());
    }
    return cb::engine_errc::failed;
}
