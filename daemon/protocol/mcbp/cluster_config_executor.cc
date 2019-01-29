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
#include <daemon/mcbp.h>
#include <daemon/memcached.h>
#include <daemon/session_cas.h>
#include <logger/logger.h>
#include <mcbp/protocol/request.h>

void get_cluster_config_executor(Cookie& cookie) {
    auto& connection = cookie.getConnection();
    auto& bucket = connection.getBucket();
    if (bucket.type == Bucket::Type::NoBucket) {
        if (connection.isXerrorSupport()) {
            cookie.setErrorContext("No bucket selected");
            cookie.sendResponse(cb::mcbp::Status::NoBucket);
        } else {
            LOG_WARNING(
                    "{}: Can't get cluster configuration without "
                    "selecting a bucket. Disconnecting {}",
                    connection.getId(),
                    connection.getDescription());
            connection.setState(StateMachine::State::closing);
        }
        return;
    }

    auto pair = bucket.clusterConfiguration.getConfiguration();
    if (pair.first == -1) {
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
    auto& connection = cookie.getConnection();
    auto& bucket = connection.getBucket();
    if (bucket.type == Bucket::Type::NoBucket) {
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

    // First validate that the provided configuration is a valid payload
    const auto& req = cookie.getRequest(Cookie::PacketContent::Full);
    auto cas = req.getCas();

    // verify that this is a legal session cas:
    if (!session_cas.increment_session_counter(cas)) {
        cookie.sendResponse(cb::mcbp::Status::KeyEexists);
        return;
    }

    try {
        auto payload = req.getValue();
        cb::const_char_buffer conf{
                reinterpret_cast<const char*>(payload.data()), payload.size()};
        bucket.clusterConfiguration.setConfiguration(conf);
        cookie.setCas(cas);
        cookie.sendResponse(cb::mcbp::Status::Success);

        const long revision =
                bucket.clusterConfiguration.getConfiguration().first;

        LOG_INFO(
                "{}: {} Updated cluster configuration for bucket [{}]. New "
                "revision: {}",
                connection.getId(),
                connection.getDescription(),
                bucket.name,
                revision);

        // Start an executor job to walk through the connections and tell
        // them to push new clustermaps
        std::shared_ptr<Task> task;
        task = std::make_shared<CccpNotificationTask>(
                connection.getBucketIndex(), revision);
        std::lock_guard<std::mutex> guard(task->getMutex());
        executorPool->schedule(task, true);
    } catch (const std::invalid_argument& e) {
        LOG_WARNING(
                "{}: {} Failed to update cluster configuration for bucket "
                "[{}] - {}",
                connection.getId(),
                connection.getDescription(),
                bucket.name,
                e.what());
        cookie.setErrorContext(e.what());
        cookie.sendResponse(cb::mcbp::Status::Einval);
    } catch (const std::exception& e) {
        LOG_WARNING(
                "{}: {} Failed to update cluster configuration for bucket "
                "[{}] - {}",
                connection.getId(),
                connection.getDescription(),
                bucket.name,
                e.what());
        cookie.setErrorContext(e.what());
        cookie.sendResponse(cb::mcbp::Status::Einternal);
    }

    session_cas.decrement_session_counter();
}
