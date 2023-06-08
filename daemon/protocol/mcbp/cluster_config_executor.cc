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
        if (cookie.getRequest().getExtdata().empty()) {
            cookie.sendResponse(cb::mcbp::Status::Success,
                                {},
                                {},
                                {active->config.data(), active->config.size()},
                                cb::mcbp::Datatype::JSON,
                                0);
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
            cookie.sendResponse(cb::mcbp::Status::Success,
                                {},
                                {},
                                {active->config.data(), active->config.size()},
                                cb::mcbp::Datatype::JSON,
                                0);
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

/// Push the configuration fot the provided bucket to all clients
/// bound to the bucket and subscribe to notifications
/// @param bucketname The name of the bucket to push
static void push_cluster_config(Bucket& bucket) {
    iterate_all_connections([&bucket](Connection& connection) -> void {
        auto mode = connection.getClustermapChangeNotification();
        if (mode == ClustermapChangeNotification::None ||
            bucket.state != Bucket::State::Ready) {
            // The client hasn't asked to be notified, or the bucket is
            // about to be deleted
            return;
        }

        if (bucket.type != BucketType::NoBucket &&
            &bucket != &connection.getBucket()) {
            // This isn't the global configuration or the selected bucket
            // so we shouldn't push the configuration
            return;
        }

        std::unique_ptr<ClusterConfiguration::Configuration> active;
        try {
            if (bucket.type == BucketType::NoBucket) {
                active = bucket.clusterConfiguration.maybeGetConfiguration({});
            } else {
                auto pushed = connection.getPushedClustermapRevno();
                active = bucket.clusterConfiguration.maybeGetConfiguration(
                        pushed);
            }

            if (!active) {
                // We've already pushed the latest version we've got
                return;
            }

            if (bucket.type == BucketType::NoBucket) {
                LOG_DEBUG("{}: Sending global Cluster map revision:  {}",
                          connection.getId(),
                          active->version);
            } else {
                connection.setPushedClustermapRevno(active->version);
                LOG_DEBUG("{}: Sending Cluster map for bucket:{} revision:{}",
                          connection.getId(),
                          bucket.name,
                          active->version);
            }

            std::string name = bucket.name;
            using namespace cb::mcbp;
            cb::mcbp::request::SetClusterConfigPayload version;
            version.setEpoch(active->version.getEpoch());
            version.setRevision(active->version.getRevno());
            size_t needed = sizeof(Request) + // packet header
                            sizeof(version) + // rev data in extdata
                            name.size(); // the name of the bucket
            if (mode == ClustermapChangeNotification::Full) {
                needed += active->config.size(); // The actual payload
            }

            std::string buffer;
            buffer.resize(needed);
            RequestBuilder builder(buffer);
            builder.setMagic(Magic::ServerRequest);
            builder.setOpcode(ServerOpcode::ClustermapChangeNotification);
            builder.setExtras(version.getBuffer());
            builder.setKey({reinterpret_cast<const uint8_t*>(name.data()),
                            name.size()});
            if (mode == ClustermapChangeNotification::Full) {
                builder.setDatatype(cb::mcbp::Datatype::JSON);
                builder.setValue({reinterpret_cast<const uint8_t*>(
                                          active->config.data()),
                                  active->config.size()});
            } else {
                builder.setDatatype(cb::mcbp::Datatype::Raw);
            }

            // Inject our packet into the stream!
            connection.copyToOutputStream(builder.getFrame()->getFrame());
        } catch (const std::bad_alloc&) {
            // memory allocation failed; just ignore the push request
            connection.shutdown();
            connection.setTerminationReason("Memory allocation failure");
        }
    });
}

void set_cluster_config_executor(Cookie& cookie) {
    // First validate that the provided configuration is a valid payload
    const auto& req = cookie.getRequest();
    auto& connection = cookie.getConnection();

    using cb::mcbp::request::SetClusterConfigPayload;
    const auto& ext = req.getCommandSpecifics<SetClusterConfigPayload>();
    const ClustermapVersion version = {ext.getEpoch(), ext.getRevision()};

    std::unique_ptr<ClusterConfiguration::Configuration> configuration;
    try {
        auto payload = req.getValue();
        std::string_view clustermap = {
                reinterpret_cast<const char*>(payload.data()), payload.size()};
        configuration = std::make_unique<ClusterConfiguration::Configuration>(
                version, clustermap);
    } catch (const std::bad_alloc&) {
        cookie.sendResponse(cb::mcbp::Status::Enomem);
        return;
    }

    auto key = req.getKey();
    const auto bucketname =
            std::string{reinterpret_cast<const char*>(key.data()), key.size()};

    // verify that this is a legal session cas:
    auto cas = req.getCas();

    cb::engine_errc status;
    if (!session_cas.execute(cas, [&status, &bucketname, &configuration]() {
            status = BucketManager::instance().setClusterConfig(
                    bucketname, std::move(configuration));
        })) {
        cookie.sendResponse(cb::mcbp::Status::KeyEexists);
        return;
    }

    if (status == cb::engine_errc::success) {
        // Log and push
        if (bucketname.empty()) {
            LOG_INFO(
                    "{}: {} Updated global cluster configuration. New "
                    "revision: {}",
                    connection.getId(),
                    connection.getDescription(),
                    version);
        } else {
            LOG_INFO(
                    "{}: Updated cluster configuration for bucket [{}]. New "
                    "revision: {}",
                    connection.getId(),
                    bucketname,
                    version);
        }
        cookie.setCas(cas);
        cookie.sendResponse(cb::mcbp::Status::Success);

        ExecutorPool::get()->schedule(std::make_shared<
                                      OneShotLimitedConcurrencyTask>(
                TaskId::Core_PushClustermapTask,
                "Push clustermap",
                [bucketname]() {
                    for (auto& bucket : all_buckets) {
                        bool thisIsTheBucket = false;
                        {
                            std::lock_guard<std::mutex> guard(bucket.mutex);
                            if (bucket.state == Bucket::State::Ready &&
                                bucket.name == bucketname) {
                                bucket.clients++;
                                thisIsTheBucket = true;
                            }
                        }

                        if (thisIsTheBucket) {
                            if (bucket.type == BucketType::NoBucket) {
                                LOG_INFO_RAW(
                                        "Pushing new global cluster "
                                        "config");
                            } else {
                                LOG_INFO(
                                        "Pushing new cluster config for "
                                        "bucket [{}]",
                                        bucket.name);
                            }
                            try {
                                push_cluster_config(bucket);
                            } catch (const std::exception& exception) {
                                LOG_WARNING(
                                        "Failed to push cluster "
                                        "configuration for bucket [{}]: {}",
                                        bucket.name,
                                        exception.what());
                            }

                            disconnect_bucket(bucket, nullptr);
                            return;
                        }
                    }
                },
                ConcurrencySemaphores::instance().cccp_notification));

        return;
    }

    LOG_WARNING(
            "{}: Failed to update cluster configuration for bucket [{}] - {}",
            connection.getId(),
            bucketname,
            status);

    cookie.sendResponse(status);
}
