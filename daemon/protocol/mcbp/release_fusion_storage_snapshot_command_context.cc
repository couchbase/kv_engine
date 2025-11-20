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
    const std::string snapshotUuid = request["snapshot_uuid"];
    const std::string bucketUuid = request["bucket_uuid"];
    const auto metadatastoreUri = request["metadatastore_uri"];
    const auto authToken = request["metadatastore_auth_token"];
    const auto fusionNamespace = generateFusionNamespace(bucketUuid);
    std::vector<magma::Magma::KVStoreID> vbucketList;
    vbucketList.reserve(request["vbucket_list"].size());
    for (const auto& id : request["vbucket_list"]) {
        vbucketList.emplace_back(id.get<magma::Magma::KVStoreID>());
    }

    const auto status =
            magma::Magma::ReleaseFusionStorageSnapshot(metadatastoreUri,
                                                       authToken,
                                                       fusionNamespace,
                                                       vbucketList,
                                                       snapshotUuid);
    if (!status.IsOK()) {
        LOG_WARNING_CTX("ReleaseFusionStorageSnapshot: ",
                        {"status", status.String()},
                        {"vbucket_list", request["vbucket_list"]},
                        {"fusion_namespace", fusionNamespace},
                        {"metadatastore_uri", metadatastoreUri},
                        {"snapshot_uuid", snapshotUuid},
                        {"bucket_uuid", bucketUuid});
        return cb::engine_errc::failed;
    }
    return cb::engine_errc::success;
}
