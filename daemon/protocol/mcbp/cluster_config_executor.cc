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
#include <daemon/cookie.h>
#include <daemon/mcaudit.h>
#include <daemon/memcached.h>
#include <daemon/one_shot_task.h>
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

/// Push the configuration fot the provided bucket to all clients
/// bound to the bucket and subscribe to notifications
/// @param bucketIndex index into the bucket array
/// @param version the version which triggered the callback
///                 (used for logging only.. we'll always push the latest
///                  version if there is a "race" where ns_server updates
///                  the clustermap while we're pushing a version)
static void push_cluster_config(unsigned int bucketIndex,
                                const ClustermapVersion& version) {
    // We've got a reference to the bucket so we need to disassociate
    // when we're done!
    auto& bucket = BucketManager::instance().at(bucketIndex);
    bool isAlive = false;

    {
        std::lock_guard<std::mutex> guard(bucket.mutex);
        isAlive = bucket.state == Bucket::State::Ready;
        if (isAlive) {
            // Bump a reference so the bucket can't be deleted while
            // we're in the middle of pushing configurations
            bucket.clients++;
        }
    }

    if (!isAlive) {
        return;
    }

    if (bucket.type == BucketType::NoBucket) {
        LOG_INFO("Pushing new global cluster config - revision:{}", version);
    } else {
        LOG_INFO(
                "Pushing new cluster config for bucket:[{}] "
                "revision:{}",
                bucket.name,
                version);
    }
    try {
        iterate_all_connections([bucketIndex](Connection& connection) -> void {
            if (!connection.isClustermapChangeNotificationSupported()) {
                // The client hasn't asked to be notified
                return;
            }

            if (bucketIndex != 0 &&
                bucketIndex != connection.getBucketIndex()) {
                // This isn't the global configuration or the selected bucket
                // so we shouldn't push the configuration
                return;
            }

            std::unique_ptr<ClusterConfiguration::Configuration> active;
            if (bucketIndex == 0) {
                active =
                        all_buckets[0]
                                .clusterConfiguration.maybeGetConfiguration({});
            } else {
                auto& bucket = connection.getBucket();
                auto pushed = connection.getPushedClustermapRevno();
                active = bucket.clusterConfiguration.maybeGetConfiguration(
                        pushed);
            }

            if (!active) {
                // We've already pushed the latest version we've got
                return;
            }

            std::string name;
            {
                std::lock_guard<std::mutex> guard(
                        all_buckets[bucketIndex].mutex);
                if (all_buckets[bucketIndex].state == Bucket::State::Ready) {
                    name = all_buckets[bucketIndex].name;
                } else {
                    // The bucket is no longer online
                    return;
                }
            }

            if (bucketIndex == 0) {
                LOG_INFO("{}: Sending global Cluster map revision:  {}",
                         connection.getId(),
                         active->version);
            } else {
                connection.setPushedClustermapRevno(active->version);
                LOG_INFO("{}: Sending Cluster map for bucket:{} revision:{}",
                         connection.getId(),
                         name,
                         active->version);
            }

            using namespace cb::mcbp;
            cb::mcbp::request::SetClusterConfigPayload version;
            version.setEpoch(active->version.getEpoch());
            version.setRevision(active->version.getRevno());
            size_t needed = sizeof(Request) + // packet header
                            sizeof(version) + // rev data in extdata
                            name.size() + // the name of the bucket
                            active->config.size(); // The actual payload
            std::string buffer;
            buffer.resize(needed);
            RequestBuilder builder(buffer);
            builder.setMagic(Magic::ServerRequest);
            builder.setDatatype(cb::mcbp::Datatype::JSON);
            builder.setOpcode(ServerOpcode::ClustermapChangeNotification);
            builder.setExtras(version.getBuffer());
            builder.setKey({reinterpret_cast<const uint8_t*>(name.data()),
                            name.size()});
            builder.setValue(
                    {reinterpret_cast<const uint8_t*>(active->config.data()),
                     active->config.size()});

            // Inject our packet into the stream!
            connection.copyToOutputStream(builder.getFrame()->getFrame());
        });
    } catch (const std::exception& e) {
        LOG_WARNING("Failed to push cluster config. Received exception: {}",
                    e.what());
    }
    disconnect_bucket(bucket, nullptr);
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
        ExecutorPool::get()->schedule(std::make_shared<OneShotTask>(
                TaskId::Core_PushClustermapTask,
                "Push clustermap",
                [bucketIndex, version]() {
                    push_cluster_config((unsigned int)bucketIndex, version);
                }));
    }

    session_cas.decrement_session_counter();
    disconnect_bucket(all_buckets[bucketIndex], nullptr);
}
