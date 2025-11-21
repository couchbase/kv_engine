/*
 *     Copyright 2025-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "delete_fusion_namespace_command_context.h"
#include <daemon/concurrency_semaphores.h>
#include <daemon/cookie.h>
#include <executor/globaltask.h>
#include <logger/logger.h>
#include <utilities/magma_support.h>

DeleteFusionNamespaceCommandContext::DeleteFusionNamespaceCommandContext(
        Cookie& cookie)
    : BackgroundThreadCommandContext(
              cookie,
              TaskId::Core_DeleteFusionNamespaceTask,
              fmt::format("Core_DeleteFusionNamespaceTask:{}",
                          cookie.getRequest().getVBucket()),
              ConcurrencySemaphores::instance().fusion_management) {
}

cb::engine_errc DeleteFusionNamespaceCommandContext::execute() {
    const auto& req = cookie.getRequest();
    const auto request = nlohmann::json::parse(req.getValueString());
    const auto logstore = request["logstore_uri"];
    const auto metadatastore = request["metadatastore_uri"];
    const auto token = request["metadatastore_auth_token"];
    const auto ns = request["namespace"];
    const auto start = std::chrono::steady_clock::now();
    const auto status = magma::Magma::DeleteFusionNamespace(
            logstore, metadatastore, token, ns);
    const auto stop = std::chrono::steady_clock::now();
    if (status.IsOK()) {
        LOG_INFO_CTX("DeleteFusionNamespace",
                     {"logstore_uri", logstore},
                     {"metadatastore_uri", metadatastore},
                     {"namespace", ns},
                     {"duration", stop - start});
        return cb::engine_errc::success;
    }
    LOG_WARNING_CTX("DeleteFusionNamespace",
                    {"logstore_uri", logstore},
                    {"metadatastore_uri", metadatastore},
                    {"namespace", ns},
                    {"error", status.String()},
                    {"duration", stop - start});
    return cb::engine_errc::failed;
}
