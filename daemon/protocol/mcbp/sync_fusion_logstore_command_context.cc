/*
 *     Copyright 2025-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#include "sync_fusion_logstore_command_context.h"

#include <daemon/buckets.h>
#include <daemon/concurrency_semaphores.h>
#include <daemon/connection.h>
#include <logger/logger.h>
#include <memcached/engine.h>

SyncFusionLogstoreCommandContext::SyncFusionLogstoreCommandContext(
        Cookie& cookie)
    : BackgroundThreadCommandContext(
              cookie,
              TaskId::SyncFusionLogstore,
              fmt::format("SyncFusionLogstore {}",
                          cookie.getRequest().getVBucket()),
              ConcurrencySemaphores::instance().fusion_management),
      vbid(cookie.getRequest().getVBucket()) {
}

cb::engine_errc SyncFusionLogstoreCommandContext::execute() {
    try {
        auto& engine = cookie.getConnection().getBucketEngine();
        return engine.syncFusionLogstore(vbid);
    } catch (const std::exception& e) {
        LOG_WARNING_CTX("SyncFusionLogstoreCommandContext: ",
                        {"error", e.what()});
        response = fmt::format("Failed: {}", e.what());
    }
    return cb::engine_errc::failed;
}
