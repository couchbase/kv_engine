/*
 *     Copyright 2017-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "executors.h"
#include <daemon/buckets.h>
#include <daemon/concurrency_semaphores.h>
#include <daemon/cookie.h>
#include <daemon/mcaudit.h>
#include <daemon/memcached.h>
#include <daemon/one_shot_limited_concurrency_task.h>
#include <daemon/session_cas.h>
#include <executor/executorpool.h>
#include <logger/logger.h>
#include <mcbp/protocol/framebuilder.h>
#include <mcbp/protocol/request.h>
#include <memcached/protocol_binary.h>

static bool check_access_to_global_config(Cookie& cookie) {
    using cb::rbac::Privilege;
    using cb::rbac::PrivilegeAccess;

    auto& conn = cookie.getConnection();
    const auto xerror = conn.isXerrorSupport();

    if (cookie.checkPrivilege(Privilege::SystemSettings).success()) {
        return true;
    }
    audit_command_access_failed(cookie);
    if (xerror) {
        cookie.sendResponse(cb::mcbp::Status::Eaccess);
    } else {
        conn.setTerminationReason("XError not enabled");
        conn.shutdown();
    }

    return false;
}

void get_cluster_config_executor(Cookie& cookie) {
    auto& connection = cookie.getConnection();
    auto& bucket = connection.getBucket();

    if (bucket.type == BucketType::NoBucket &&
        !check_access_to_global_config(cookie)) {
        // Error reason already logged (and next state set)
        return;
    }

    auto active = bucket.clusterConfiguration.maybeGetConfiguration({});
    if (active) {
        if (cookie.getRequest().getExtdata().empty()) {
            if (connection.supportsSnappyEverywhere()) {
                cookie.sendResponse(cb::mcbp::Status::Success,
                                    {},
                                    {},
                                    active->compressed,
                                    cb::mcbp::Datatype::SnappyCompressedJson,
                                    0);
            } else {
                cookie.sendResponse(cb::mcbp::Status::Success,
                                    {},
                                    {},
                                    active->uncompressed,
                                    cb::mcbp::Datatype::JSON,
                                    0);
            }
            connection.setPushedClustermapRevno(active->version);
            return;
        }
        using cb::mcbp::request::GetClusterConfigPayload;
        const auto& payload =
                cookie.getRequest()
                        .getCommandSpecifics<GetClusterConfigPayload>();

        const ClustermapVersion version{payload.getEpoch(),
                                        payload.getRevision()};
        connection.setPushedClustermapRevno(active->version);
        if (version < active->version) {
            // The client has an older version, return our version
            if (connection.supportsSnappyEverywhere()) {
                cookie.sendResponse(cb::mcbp::Status::Success,
                                    {},
                                    {},
                                    active->compressed,
                                    cb::mcbp::Datatype::SnappyCompressedJson,
                                    0);
            } else {
                cookie.sendResponse(cb::mcbp::Status::Success,
                                    {},
                                    {},
                                    active->uncompressed,
                                    cb::mcbp::Datatype::JSON,
                                    0);
            }
            return;
        }
        // The client knows this (or a more recent) version.
        // Send an empty response
        cookie.sendResponse(cb::mcbp::Status::Success,
                            {},
                            {},
                            {},
                            cb::mcbp::Datatype::Raw,
                            0);
        if (version > active->version) {
            // The client provided a more recent version that we know of,
            // so we can skip sending older revisions to the client
            connection.setPushedClustermapRevno(version);
        }
    } else {
        cookie.sendResponse(cb::mcbp::Status::KeyEnoent);
    }
}
