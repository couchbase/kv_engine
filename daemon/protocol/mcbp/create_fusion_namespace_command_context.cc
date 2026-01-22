/*
 *     Copyright 2026-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "create_fusion_namespace_command_context.h"

#include "daemon/connection.h"
#include <daemon/concurrency_semaphores.h>
#include <daemon/cookie.h>
#include <executor/globaltask.h>
#include <logger/logger.h>
#include <utilities/fusion_utilities.h>
#include <utilities/magma_support.h>

CreateFusionNamespaceCommandContext::CreateFusionNamespaceCommandContext(
        Cookie& cookie)
    : BackgroundThreadCommandContext(
              cookie,
              TaskId::Core_CreateFusionNamespaceTask,
              fmt::format("Core_CreateFusionNamespaceTask:{}",
                          cookie.getRequest().getVBucket()),
              ConcurrencySemaphores::instance().fusion_management) {
}

cb::engine_errc CreateFusionNamespaceCommandContext::execute() {
    try {
        auto& engine = cookie.getConnection().getBucketEngine();
        return engine.createFusionNamespace();
    } catch (const std::exception& e) {
        LOG_WARNING_CTX("CreateFusionNamespaceCommandContext: ",
                        {"error", e.what()});
        response = fmt::format("Failed: {}", e.what());
    }
    return cb::engine_errc::failed;
}
