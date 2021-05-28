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
#include <daemon/cookie.h>
#include <daemon/mcaudit.h>
#include <daemon/memcached.h>
#include <daemon/session_cas.h>
#include <executor/executor.h>
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

/// Push the configuration fot the provided bucket to all clients
/// bound to the bucket and subscribe to notifications
/// @param bucketIndex index into the bucket array
/// @param revision the revision which triggered the callback
///                 (used for logging only.. we'll always push the latest
///                  version if there is a "race" where ns_server updates
///                  the clustermap while we're pushing a version)
static void push_cluster_config(int bucketIndex, int revision) {
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
        LOG_INFO("Pushing new global cluster config - revision:[{}]", revision);
    } else {
        LOG_INFO(
                "Pushing new cluster config for bucket:[{}] "
                "revision:[{}]",
                bucket.name,
                revision);
    }
    try {
        iterate_all_connections([](Connection& connection) -> void {
            if (!connection.isClustermapChangeNotificationSupported()) {
                // The client hasn't asked to be notified
                return;
            }

            auto& b = connection.getBucket();
            auto payload = b.clusterConfiguration.getConfiguration();
            if (payload.first < connection.getClustermapRevno()) {
                // The client has a newer revision map, ignore this one
                return;
            }

            connection.setClustermapRevno(payload.first);
            LOG_INFO("{}: Sending Cluster map revision {}",
                     connection.getId(),
                     payload.first);

            std::string name = b.name;
            size_t needed = sizeof(cb::mcbp::Request) + // packet header
                            4 + // rev number in extdata
                            name.size() + // the name of the bucket
                            payload.second->size(); // The actual payload
            std::string buffer;
            buffer.resize(needed);
            cb::mcbp::RequestBuilder builder(buffer);
            builder.setMagic(cb::mcbp::Magic::ServerRequest);
            builder.setDatatype(cb::mcbp::Datatype::JSON);
            builder.setOpcode(
                    cb::mcbp::ServerOpcode::ClustermapChangeNotification);

            // The extras contains the cluster revision number as an
            // uint32_t
            const uint32_t rev = htonl(payload.first);
            builder.setExtras(
                    {reinterpret_cast<const uint8_t*>(&rev), sizeof(rev)});
            builder.setKey({reinterpret_cast<const uint8_t*>(name.data()),
                            name.size()});
            builder.setValue(
                    {reinterpret_cast<const uint8_t*>(payload.second->data()),
                     payload.second->size()});

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

    int revision;
    int bucketIndex = -1;
    auto payload = req.getValue();
    std::string_view clustermap = {
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
                connection.shutdown();
                connection.setTerminationReason("XError not enabled");
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
        disconnect_bucket(all_buckets[bucketIndex], nullptr);
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
        cb::executor::get().add([bucketIndex, revision]() {
            push_cluster_config(bucketIndex, revision);
        });
    }

    session_cas.decrement_session_counter();
    disconnect_bucket(all_buckets[bucketIndex], nullptr);
}
