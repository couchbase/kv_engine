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
#include <daemon/one_shot_limited_concurrency_task.h>
#include <executor/executorpool.h>
#include <memcached/engine.h>

SyncFusionLogstoreCommandContext::SyncFusionLogstoreCommandContext(
        Cookie& cookie)
    : SteppableCommandContext(cookie), vbid(cookie.getRequest().getVBucket()) {
}
cb::engine_errc SyncFusionLogstoreCommandContext::step() {
    if (completed) {
        // Note: SteppableCommandContext::drive handles non-successful
        // executions and doesn't execute here at all in that case
        cookie.sendResponse(cb::engine_errc::success);
        return cb::engine_errc::success;
    }

    const auto func = [this]() -> void {
        try {
            completed = true;
            auto& engine = cookie.getConnection().getBucketEngine();
            const auto res = engine.syncFusionLogstore(vbid);
            cookie.notifyIoComplete(res);
        } catch (const std::exception& e) {
            LOG_WARNING_CTX("SyncFusionLogstoreCommandContext: ",
                            {"error", e.what()});
            cookie.setErrorContext(e.what());
            cookie.notifyIoComplete(cb::engine_errc::failed);
        }
    };
    const auto task = std::make_shared<OneShotLimitedConcurrencyTask>(
            TaskId::SyncFusionLogstore,
            "SyncFusionLogstore " + to_string(vbid),
            func,
            ConcurrencySemaphores::instance().fusion_management);
    ExecutorPool::get()->schedule(task);
    return cb::engine_errc::would_block;
}
