/*
 *     Copyright 2025-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "get_fusion_storage_snapshot_command_context.h"

#include <daemon/concurrency_semaphores.h>
#include <daemon/connection.h>
#include <logger/logger.h>
#include <utilities/fusion_utilities.h>
#include <utilities/magma_support.h>

GetFusionStorageSnapshotCommandContext::GetFusionStorageSnapshotCommandContext(
        Cookie& cookie)
    : BackgroundThreadCommandContext(
              cookie,
              TaskId::Core_GetFusionStorageSnapshotTask,
              fmt::format("Core_GetFusionStorageSnapshotTask:{}",
                          cookie.getRequest().getVBucket()),
              ConcurrencySemaphores::instance().fusion_management) {
}

cb::engine_errc GetFusionStorageSnapshotCommandContext::execute() {
    const auto& req = cookie.getRequest();
    const auto request = nlohmann::json::parse(req.getValueString());
    const std::string snapshotUuid = request["snapshot_uuid"];
    const auto bucketUuid = request["bucket_uuid"];
    const auto fusionNamespace = generateFusionNamespace(bucketUuid);
    const std::time_t validity = request["valid_till"];
    const auto validTill = std::chrono::system_clock::from_time_t(validity);
    const auto metadatastoreUri = request["metadatastore_uri"];
    const auto authToken = request["metadatastore_auth_token"];
    std::vector<magma::Magma::KVStoreID> vbucketList;
    vbucketList.reserve(request["vbucket_list"].size());
    for (const auto& id : request["vbucket_list"]) {
        vbucketList.emplace_back(id.get<magma::Magma::KVStoreID>());
    }

    const auto [status, json] =
            magma::Magma::GetFusionStorageSnapshot(metadatastoreUri,
                                                   authToken,
                                                   fusionNamespace,
                                                   vbucketList,
                                                   snapshotUuid,
                                                   validTill);
    if (!status.IsOK()) {
        LOG_WARNING_CTX("GetFusionStorageSnapshot: ",
                        {"status", status.String()},
                        {"vbucket_list", request["vbucket_list"]},
                        {"fusion_namespace", fusionNamespace},
                        {"metadatastore_uri", metadatastoreUri},
                        {"snapshot_uuid", snapshotUuid},
                        {"bucket_uuid", bucketUuid},
                        {"valid_till", validity});
        return cb::engine_errc::failed;
    }
    response = json.dump();
    datatype = cb::mcbp::Datatype::JSON;
    return cb::engine_errc::success;
}
