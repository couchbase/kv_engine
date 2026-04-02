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
    const std::string snapshotUuid = request[fusion_json_key_snapshot_uuid];
    const auto bucketUuid = request[fusion_json_key_bucket_uuid];
    const auto fusionNamespace = generateFusionNamespace(bucketUuid);
    const std::time_t validity = request[fusion_json_key_valid_till];
    const auto validTill = std::chrono::system_clock::from_time_t(validity);
    const auto metadatastoreUri = request[fusion_json_key_metadatastore_uri];
    const auto authToken = request[fusion_json_key_metadatastore_auth_token];
    std::vector<magma::Magma::KVStoreID> vbucketList;
    vbucketList.reserve(request[fusion_json_key_vbucket_list].size());
    for (const auto& id : request[fusion_json_key_vbucket_list]) {
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
        response = fmt::format("Failed with error: {}", status.String());
        LOG_WARNING_CTX("GetFusionStorageSnapshot: ",
                        {"status", status.String()},
                        {fusion_json_key_vbucket_list,
                         request[fusion_json_key_vbucket_list]},
                        {"fusion_namespace", fusionNamespace},
                        {fusion_json_key_metadatastore_uri, metadatastoreUri},
                        {fusion_json_key_snapshot_uuid, snapshotUuid},
                        {fusion_json_key_bucket_uuid, bucketUuid},
                        {fusion_json_key_valid_till, validity});
        return cb::engine_errc::failed;
    }
    response = json.dump();
    datatype = cb::mcbp::Datatype::JSON;
    return cb::engine_errc::success;
}
