/*
 *     Copyright 2025-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "delete_fusion_namespaces_command_context.h"

#include <daemon/concurrency_semaphores.h>
#include <daemon/cookie.h>
#include <executor/globaltask.h>
#ifdef USE_FUSION
#include <libmagma/magma.h>
#endif
#include <logger/logger.h>

DeleteFusionNamespacesCommandContext::DeleteFusionNamespacesCommandContext(
        Cookie& cookie)
    : BackgroundThreadCommandContext(
              cookie,
              TaskId::Core_DeleteFusionNamespacesTask,
              fmt::format("Core_DeleteFusionNamespacesTask:{}",
                          cookie.getRequest().getVBucket()),
              ConcurrencySemaphores::instance().fusion_management) {
}

cb::engine_errc DeleteFusionNamespacesCommandContext::execute() {
#ifdef USE_FUSION
    const auto& req = cookie.getRequest();
    const auto request = nlohmann::json::parse(req.getValueString());
    const auto logstore = request["logstore_uri"];
    const auto metadatastore = request["metadatastore_uri"];
    const auto token = request["metadatastore_auth_token"];
    const auto namespaces = request["namespaces"];
    const auto status = magma::Magma::DeleteFusionNamespacesExcept(
            logstore, metadatastore, token, namespaces);
    if (status.ErrorCode() != magma::Status::Code::Ok) {
        LOG_WARNING_CTX("DeleteFusionNamespacesExcept: ",
                        {"logstore_uri", logstore},
                        {"metadatastore_uri", metadatastore},
                        {"namespaces", namespaces},
                        {"error", status.String()});
        return cb::engine_errc::failed;
    }
    return cb::engine_errc::success;
#else
    return cb::engine_errc::not_supported;
#endif
}
