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

#include <cblogger/logger.h>
#include <daemon/buckets.h>
#include <daemon/concurrency_semaphores.h>
#include <daemon/connection.h>
#include <daemon/one_shot_limited_concurrency_task.h>
#include <executor/executorpool.h>
#include <memcached/engine.h>
#include <snapshot/disk_format_constraint.h>

PrepareSnapshotContext::PrepareSnapshotContext(Cookie& cookie)
    : BackgroundThreadCommandContext(
              cookie,
              TaskId::Core_PrepareSnapshotTask,
              "Prepare Snapshot for " +
                      cookie.getRequest().getVBucket().to_string(),
              ConcurrencySemaphores::instance()
                      .encryption_and_snapshot_management),
      vb(cookie.getRequest().getVBucket()),
      value(cookie.getRequest().getValueString()) {
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
                        {"bucket", connection.getBucket().name},
                        {"vb", vb},
                        {"error", e.what()});
        response = fmt::format("Failed: {}", e.what());
    }

    return cb::engine_errc::failed;
}

cb::engine_errc PrepareSnapshotContext::doCreateSnapshot() {
    cb::snapshot::DiskFormatConstraint constraint;
    try {
        const auto json = nlohmann::json::parse(value);
        if (!json.contains("storage") || !json["storage"].is_object()) {
            throw std::invalid_argument("storage must be present as an object");
        }
        constraint = json["storage"];
    } catch (const std::exception& e) {
        cookie.setErrorContext(
                fmt::format("Invalid disk format constraint: {}", e.what()));
        return cb::engine_errc::invalid_arguments;
    }

    auto& engine = connection.getBucket().getEngine();
    return engine.prepare_snapshot(
            cookie, vb, constraint, [this](const nlohmann::json& json) {
                response = json.dump();
            });
}
