/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2017 Couchbase, Inc.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

#include "executors.h"
#include <daemon/buckets.h>
#include <daemon/cccp_notification_task.h>
#include <daemon/cookie.h>
#include <daemon/executorpool.h>
#include <daemon/mcaudit.h>
#include <daemon/mcbp.h>
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

    switch (conn.checkPrivilege(Privilege::SystemSettings, cookie)) {
    case PrivilegeAccess::Ok:
        return true;

    case PrivilegeAccess::Fail:
        LOG_WARNING("{} {}: no access to Global Cluster Config.{}",
                    conn.getId(),
                    conn.getDescription(),
                    xerror ? "" : " XError not enabled, closing connection");
        audit_command_access_failed(cookie);
        if (xerror) {
            cookie.sendResponse(cb::mcbp::Status::Eaccess);
        } else {
            conn.setState(StateMachine::State::closing);
        }

        return false;
    case PrivilegeAccess::Stale:
        LOG_WARNING("{}: Stale authentication context.{}",
                    conn.getId(),
                    xerror ? "" : " XError not enabled, closing connection");
        if (xerror) {
            cookie.sendResponse(cb::mcbp::Status::AuthStale);
        } else {
            conn.setState(StateMachine::State::closing);
        }

        return false;
    }

    throw std::invalid_argument(
            "check_access_to_global_config(): Invalid value returned from "
            "checkPrivilege)");
}

void get_cluster_config_executor(Cookie& cookie) {
    auto& connection = cookie.getConnection();
    auto& bucket = connection.getBucket();

    if (bucket.type == BucketType::NoBucket &&
        !check_access_to_global_config(cookie)) {
        // Error reason already logged (and next state set)
        return;
    }

    auto pair = bucket.clusterConfiguration.getConfiguration();
    if (pair.first == ClusterConfiguration::NoConfiguration) {
        cookie.sendResponse(cb::mcbp::Status::KeyEnoent);
    } else {
        cookie.sendResponse(cb::mcbp::Status::Success,
                            {},
                            {},
                            {pair.second->data(), pair.second->size()},
                            cb::mcbp::Datatype::JSON,
                            0);
        connection.setClustermapRevno(pair.first);
    }
}

void set_cluster_config_executor(Cookie& cookie) {
    // First validate that the provided configuration is a valid payload
    const auto& req = cookie.getRequest(Cookie::PacketContent::Full);
    auto& connection = cookie.getConnection();

    int revision;
    int bucketIndex = -1;
    auto payload = req.getValue();
    cb::const_char_buffer clustermap = {
            reinterpret_cast<const char*>(payload.data()), payload.size()};

    // Is this a new or an old-style message
    auto extras = req.getExtdata();
    if (extras.empty()) {
        // This is an old style command
        auto& bucket = connection.getBucket();
        if (bucket.type == BucketType::NoBucket) {
            if (connection.isXerrorSupport()) {
                cookie.setErrorContext("No bucket selected");
                cookie.sendResponse(cb::mcbp::Status::NoBucket);
            } else {
                LOG_WARNING(
                        "{}: Can't set cluster configuration without "
                        "selecting a bucket. Disconnecting {}",
                        connection.getId(),
                        connection.getDescription());
                connection.setState(StateMachine::State::closing);
            }
            return;
        }
        bucketIndex = connection.getBucketIndex();
        revision = ClusterConfiguration::getRevisionNumber(clustermap);
        if (revision < 0) {
            cookie.setErrorContext(
                    "Revision must be specified as a positive number");
            cookie.sendResponse(cb::mcbp::Status::Einval);
            return;
        }
        auto& b = connection.getBucket();
        std::lock_guard<std::mutex> guard(b.mutex);
        b.clients++;
    } else {
        // Locate bucket to operate
        auto key = req.getKey();
        const auto bucketname = std::string{
                reinterpret_cast<const char*>(key.data()), key.size()};
        for (size_t ii = 0; ii < all_buckets.size() && bucketIndex == -1;
             ++ii) {
            Bucket& b = all_buckets.at(ii);
            std::lock_guard<std::mutex> guard(b.mutex);
            if (b.state == Bucket::State::Ready &&
                strcmp(b.name, bucketname.c_str()) == 0) {
                b.clients++;
                bucketIndex = int(ii);
            }
        }
        const auto& ext = *reinterpret_cast<
                const cb::mcbp::request::SetClusterConfigPayload*>(
                extras.data());
        revision = ext.getRevision();
    }

    if (bucketIndex == -1) {
        cookie.sendResponse(cb::mcbp::Status::KeyEnoent);
        return;
    }

    // verify that this is a legal session cas:
    auto cas = req.getCas();
    if (!session_cas.increment_session_counter(cas)) {
        cookie.sendResponse(cb::mcbp::Status::KeyEexists);
        std::lock_guard<std::mutex> guard(all_buckets[bucketIndex].mutex);
        all_buckets[bucketIndex].clients--;
        return;
    }

    bool failed = false;
    try {
        all_buckets[bucketIndex].clusterConfiguration.setConfiguration(
                clustermap, revision);
        if (bucketIndex == 0) {
            LOG_INFO(
                    "{}: {} Updated global cluster configuration. New "
                    "revision: {}",
                    connection.getId(),
                    connection.getDescription(),
                    revision);
        } else {
            LOG_INFO(
                    "{}: {} Updated cluster configuration for bucket [{}]. New "
                    "revision: {}",
                    connection.getId(),
                    connection.getDescription(),
                    all_buckets[bucketIndex].name,
                    revision);
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
            task = std::make_shared<CccpNotificationTask>(
                    connection.getBucketIndex(), revision);
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
    std::lock_guard<std::mutex> guard(all_buckets[bucketIndex].mutex);
    all_buckets[bucketIndex].clients--;
}
