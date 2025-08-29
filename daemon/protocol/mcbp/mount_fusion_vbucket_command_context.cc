/*
 *     Copyright 2025-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "mount_fusion_vbucket_command_context.h"

#include <daemon/connection.h>

MountFusionVbucketCommandContext::MountFusionVbucketCommandContext(
        Cookie& cookie)
    : SteppableCommandContext(cookie), state(State::Mount) {
    const auto& req = cookie.getRequest();
    const auto json = nlohmann::json::parse(req.getValueString());
    source = VBucketSnapshotSource::FusionGuestVolumes;
    if (json.contains("source")) {
        source = json.at("source");
    }
    if (source == VBucketSnapshotSource::FusionGuestVolumes) {
        paths = json.at("mountPaths");
    } else if (source == VBucketSnapshotSource::FusionLogStore) {
        paths.push_back(json.at("snapshotUUID").get<std::string>());
    }
}

cb::engine_errc MountFusionVbucketCommandContext::step() {
    auto ret = cb::engine_errc::failed;
    do {
        switch (state) {
        case State::Mount:
            ret = mount();
            break;
        case State::SendResponse:
            ret = sendResponse();
            break;
        case State::Done:
            return cb::engine_errc::success;
        }
    } while (ret == cb::engine_errc::success);
    return ret;
}

cb::engine_errc MountFusionVbucketCommandContext::mount() {
    auto& connection = cookie.getConnection();
    auto ret = connection.getBucketEngine().mountVBucket(
            cookie,
            cookie.getRequest().getVBucket(),
            source,
            paths,
            [this](const nlohmann::json& json) {
                // The engine needs to use NonBucketAllocationGuard
                response = json.dump();
            });
    if (ret == cb::engine_errc::success) {
        state = State::SendResponse;
    }
    return ret;
}

cb::engine_errc MountFusionVbucketCommandContext::sendResponse() {
    cookie.sendResponse(cb::mcbp::Status::Success,
                        {},
                        {},
                        response,
                        cb::mcbp::Datatype::JSON,
                        cb::mcbp::cas::Wildcard);
    state = State::Done;
    return cb::engine_errc::success;
}
