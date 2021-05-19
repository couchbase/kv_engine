/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
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
#include <daemon/cccp_notification_task.h>
#include <daemon/cookie.h>
#include <daemon/executorpool.h>
#include <daemon/mcaudit.h>
#include <daemon/memcached.h>
#include <daemon/session_cas.h>
#include <logger/logger.h>
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
    LOG_WARNING("{} {}: no access to Global Cluster Config.{}",
                conn.getId(),
                conn.getDescription(),
                xerror ? "" : " XError not enabled, closing connection");
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
        cookie.sendResponse(cb::mcbp::Status::Success,
                            {},
                            {},
                            {active->config.data(), active->config.size()},
                            cb::mcbp::Datatype::JSON,
                            0);
        connection.setPushedClustermapRevno(active->version);
    } else {
        cookie.sendResponse(cb::mcbp::Status::KeyEnoent);
    }
}

void set_cluster_config_executor(Cookie& cookie) {
    // First validate that the provided configuration is a valid payload
    const auto& req = cookie.getRequest();
    auto& connection = cookie.getConnection();

    int bucketIndex = -1;
    // Locate bucket to operate
    auto key = req.getKey();
    const auto bucketname =
            std::string{reinterpret_cast<const char*>(key.data()), key.size()};
    for (size_t ii = 0; ii < all_buckets.size() && bucketIndex == -1; ++ii) {
        Bucket& b = all_buckets.at(ii);
        std::lock_guard<std::mutex> guard(b.mutex);
        if (b.state == Bucket::State::Ready &&
            strcmp(b.name, bucketname.c_str()) == 0) {
            b.clients++;
            bucketIndex = int(ii);
        }
    }

    if (bucketIndex == -1) {
        cookie.sendResponse(cb::mcbp::Status::KeyEnoent);
        return;
    }

    // verify that this is a legal session cas:
    auto cas = req.getCas();
    if (!session_cas.increment_session_counter(cas)) {
        cookie.sendResponse(cb::mcbp::Status::KeyEexists);
        disconnect_bucket(all_buckets[bucketIndex], nullptr);
        return;
    }

    ClustermapVersion version;

    // Is this a new or an old-style message
    auto extras = req.getExtdata();
    using cb::mcbp::request::DeprecatedSetClusterConfigPayload;
    using cb::mcbp::request::SetClusterConfigPayload;
    if (extras.size() == sizeof(DeprecatedSetClusterConfigPayload)) {
        // @todo remove once ns_server is up to date!
        const auto& ext = *reinterpret_cast<
                const cb::mcbp::request::DeprecatedSetClusterConfigPayload*>(
                extras.data());
        version = {0, ext.getRevision()};
    } else {
        const auto& ext = *reinterpret_cast<
                const cb::mcbp::request::SetClusterConfigPayload*>(
                extras.data());
        version = {ext.getEpoch(), ext.getRevision()};
    }

    bool failed = false;
    try {
        auto payload = req.getValue();
        std::string_view clustermap = {
                reinterpret_cast<const char*>(payload.data()), payload.size()};
        all_buckets[bucketIndex].clusterConfiguration.setConfiguration(
                std::make_unique<ClusterConfiguration::Configuration>(
                        version, clustermap));
        if (bucketIndex == 0) {
            LOG_INFO(
                    "{}: {} Updated global cluster configuration. New "
                    "revision: {}",
                    connection.getId(),
                    connection.getDescription(),
                    version);
        } else {
            LOG_INFO(
                    "{}: {} Updated cluster configuration for bucket [{}]. New "
                    "revision: {}",
                    connection.getId(),
                    connection.getDescription(),
                    all_buckets[bucketIndex].name,
                    version);
        }
        cookie.setCas(cas);
        cookie.sendResponse(cb::mcbp::Status::Success);
    } catch (const std::exception& e) {
        LOG_WARNING(
                "{}: {} Failed to update cluster configuration for bucket "
                "[{}] - {}",
                connection.getId(),
                connection.getDescription(),
                all_buckets[bucketIndex].name,
                e.what());
        failed = true;
    }

    if (!failed) {
        try {
            // Start an executor job to walk through the connections and tell
            // them to push new clustermaps
            std::shared_ptr<Task> task;
            task = std::make_shared<CccpNotificationTask>(bucketIndex, version);
            std::lock_guard<std::mutex> guard(task->getMutex());
            executorPool->schedule(task, true);
        } catch (const std::exception& e) {
            LOG_WARNING(
                    "{}: {} Failed to push cluster notifications for bucket "
                    "[{}] - {}",
                    connection.getId(),
                    connection.getDescription(),
                    all_buckets[bucketIndex].name,
                    e.what());
        }
    }

    session_cas.decrement_session_counter();
    disconnect_bucket(all_buckets[bucketIndex], nullptr);
}
