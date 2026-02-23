/*
 *     Copyright 2024-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "prepare_snapshot_context.h"

#include <daemon/buckets.h>
#include <daemon/concurrency_semaphores.h>
#include <daemon/connection.h>
#include <daemon/one_shot_limited_concurrency_task.h>
#include <executor/executorpool.h>
#include <logger/logger.h>
#include <memcached/engine.h>

PrepareSnapshotContext::PrepareSnapshotContext(Cookie& cookie)
    : BackgroundThreadCommandContext(
              cookie,
              TaskId::Core_PrepareSnapshotTask,
              "Prepare Snapshot for " +
                      cookie.getRequest().getVBucket().to_string(),
              ConcurrencySemaphores::instance()
                      .encryption_and_snapshot_management),
      vb(cookie.getRequest().getVBucket()) {
}

cb::engine_errc PrepareSnapshotContext::execute() {
    try {
        auto ret = doCreateSnapshot();
        if (!response.empty()) {
            datatype = cb::mcbp::Datatype::JSON;
        }
        return ret;
    } catch (const std::exception& e) {
        LOG_WARNING_CTX("Exception occurred while preparing snapshot",
                        {"conn_id", connection.getId()},
                        {"vb", vb},
                        {"error", e.what()});
        response = fmt::format("Failed: {}", e.what());
    }

    return cb::engine_errc::failed;
}

cb::engine_errc PrepareSnapshotContext::doCreateSnapshot() {
    auto& engine = connection.getBucket().getEngine();
    return engine.prepare_snapshot(
            cookie, vb, [this](const nlohmann::json& json) {
                response = json.dump();
            });
}
