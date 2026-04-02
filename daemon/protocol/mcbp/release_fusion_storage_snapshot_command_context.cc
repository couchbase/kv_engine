/*
 *     Copyright 2025-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "release_fusion_storage_snapshot_command_context.h"

#include <daemon/concurrency_semaphores.h>
#include <daemon/connection.h>
#include <logger/logger.h>
#include <utilities/fusion_utilities.h>
#include <utilities/magma_support.h>

ReleaseFusionStorageSnapshotCommandContext::
        ReleaseFusionStorageSnapshotCommandContext(Cookie& cookie)
    : BackgroundThreadCommandContext(
              cookie,
              TaskId::Core_ReleaseFusionStorageSnapshotTask,
              fmt::format("Core_ReleaseFusionStorageSnapshotTask:{}",
                          cookie.getRequest().getVBucket()),
              ConcurrencySemaphores::instance().fusion_management) {
}

cb::engine_errc ReleaseFusionStorageSnapshotCommandContext::execute() {
    const auto& req = cookie.getRequest();
    const auto request = nlohmann::json::parse(req.getValueString());
    const std::string snapshotUuid = request[fusion_json_key_snapshot_uuid];
    const std::string bucketUuid = request[fusion_json_key_bucket_uuid];
    const auto metadatastoreUri = request[fusion_json_key_metadatastore_uri];
    const auto authToken = request[fusion_json_key_metadatastore_auth_token];
    const auto fusionNamespace = generateFusionNamespace(bucketUuid);
    std::vector<magma::Magma::KVStoreID> vbucketList;
    vbucketList.reserve(request[fusion_json_key_vbucket_list].size());
    for (const auto& id : request[fusion_json_key_vbucket_list]) {
        vbucketList.emplace_back(id.get<magma::Magma::KVStoreID>());
    }

    const auto status =
            magma::Magma::ReleaseFusionStorageSnapshot(metadatastoreUri,
                                                       authToken,
                                                       fusionNamespace,
                                                       vbucketList,
                                                       snapshotUuid);
    if (!status.IsOK()) {
        response = fmt::format("Failed with error: {}", status.String());
        LOG_WARNING_CTX("ReleaseFusionStorageSnapshot: ",
                        {"status", status.String()},
                        {fusion_json_key_vbucket_list,
                         request[fusion_json_key_vbucket_list]},
                        {"fusion_namespace", fusionNamespace},
                        {fusion_json_key_metadatastore_uri, metadatastoreUri},
                        {fusion_json_key_snapshot_uuid, snapshotUuid},
                        {fusion_json_key_bucket_uuid, bucketUuid});
        return cb::engine_errc::failed;
    }
    return cb::engine_errc::success;
}
