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
    : SteppableCommandContext(cookie), vb(cookie.getRequest().getVBucket()) {
}

cb::engine_errc PrepareSnapshotContext::step() {
    auto ret = cb::engine_errc::success;
    while (ret == cb::engine_errc::success) {
        switch (state) {
        case State::Initialize:
            ret = initialize();
            break;
        case State::Done:
            return done();
        }
    }
    return ret;
}

cb::engine_errc PrepareSnapshotContext::initialize() {
    ExecutorPool::get()->schedule(
            std::make_shared<OneShotLimitedConcurrencyTask>(
                    TaskId::Core_PrepareSnapshotTask,
                    "Prepare Snapshot",
                    [this]() {
                        try {
                            cookie.notifyIoComplete(doCreateSnapshot());
                        } catch (const std::exception& e) {
                            cookie.setErrorContext(
                                    fmt::format("Failed: {}", e.what()));
                            cookie.notifyIoComplete(cb::engine_errc::failed);
                        }
                    },
                    ConcurrencySemaphores::instance()
                            .encryption_and_snapshot_management));

    return cb::engine_errc::would_block;
}

cb::engine_errc PrepareSnapshotContext::doCreateSnapshot() {
    auto& engine = connection.getBucket().getEngine();
    state = State::Done;
    return engine.prepare_snapshot(
            cookie, vb, [this](const nlohmann::json& json) {
                snapshot = json;
            });
}

cb::engine_errc PrepareSnapshotContext::done() {
    cookie.sendResponse(cb::mcbp::Status::Success,
                        {},
                        {},
                        snapshot.rlock()->dump(),
                        cb::mcbp::Datatype::JSON,
                        0);
    return cb::engine_errc::success;
}
