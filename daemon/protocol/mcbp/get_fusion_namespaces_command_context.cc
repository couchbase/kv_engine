/*
 *     Copyright 2025-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "get_fusion_namespaces_command_context.h"
#include <daemon/concurrency_semaphores.h>
#include <daemon/cookie.h>
#include <executor/globaltask.h>
#include <logger/logger.h>
#include <utilities/fusion_utilities.h>
#include <utilities/magma_support.h>

GetFusionNamespacesCommandContext::GetFusionNamespacesCommandContext(
        Cookie& cookie)
    : BackgroundThreadCommandContext(
              cookie,
              TaskId::Core_GetFusionNamespacesTask,
              fmt::format("Core_GetFusionNamespacesTask:{}",
                          cookie.getRequest().getVBucket()),
              ConcurrencySemaphores::instance().fusion_management) {
}

cb::engine_errc GetFusionNamespacesCommandContext::execute() {
    const auto& req = cookie.getRequest();
    const auto request = nlohmann::json::parse(req.getValueString());
    const auto metadatastore = request["metadatastore_uri"];
    const auto token = request["metadatastore_auth_token"];
    const auto [status, json] =
            magma::Magma::GetFusionNamespaces(metadatastore,
                                              token,
                                              magma_fusion_namespace_prefix,
                                              magma_fusion_namespace_depth);
    if (status.IsOK()) {
        response = json.dump();
        datatype = cb::mcbp::Datatype::JSON;
        return cb::engine_errc::success;
    }
    LOG_WARNING_CTX("GetFusionNamespaces",
                    {"metadatastore_uri", metadatastore},
                    {"error", status.String()});
    cookie.setErrorContext(
            fmt::format("Failed with error: {}", status.String()));
    return cb::engine_errc::failed;
}
