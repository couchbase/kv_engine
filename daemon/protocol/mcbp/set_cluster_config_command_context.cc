/*
 *     Copyright 2023-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "set_cluster_config_command_context.h"
#include "daemon/buckets.h"
#include "mcbp/protocol/framebuilder.h"

#include <cbsasl/mechanism.h>
#include <daemon/concurrency_semaphores.h>
#include <daemon/connection.h>
#include <daemon/one_shot_limited_concurrency_task.h>
#include <executor/executorpool.h>
#include <folly/io/IOBuf.h>
#include <logger/logger.h>
#include <platform/compress.h>

using cb::mcbp::request::SetClusterConfigPayload;

static ClustermapVersion getVersion(const SetClusterConfigPayload& payload) {
    return ClustermapVersion{payload.getEpoch(), payload.getRevision()};
}

SetClusterConfigCommandContext::SetClusterConfigCommandContext(Cookie& cookie)
    : SteppableCommandContext(cookie),
      bucketname(cookie.getRequest().getKeyString()),
      sessiontoken(cookie.getRequest().getCas()),
      version(getVersion(
              cookie.getRequest()
                      .getCommandSpecifics<SetClusterConfigPayload>())),
      uncompressed(cookie.getRequest().getValueString()) {
}

cb::engine_errc SetClusterConfigCommandContext::step() {
    switch (state) {
    case State::ScheduleTask:
        ExecutorPool::get()->schedule(
                std::make_shared<OneShotLimitedConcurrencyTask>(
                        TaskId::Core_SetClusterConfig,
                        "Set cluster configuration",
                        [this]() {
                            try {
                                cookie.notifyIoComplete(doSetClusterConfig());
                            } catch (const std::bad_alloc&) {
                                cookie.notifyIoComplete(
                                        cb::engine_errc::no_memory);
                            }
                        },
                        ConcurrencySemaphores::instance()
                                .compress_cluster_config));
        state = State::Done;
        return cb::engine_errc::would_block;
    case State::Done:
        return done();
    }
    throw std::runtime_error(
            "SetClusterConfigCommandContext::step() Invalid state");
}

cb::engine_errc SetClusterConfigCommandContext::doSetClusterConfig() {
    std::string compressed;
    try {
        const auto iob = cb::compression::deflateSnappy(uncompressed);
        compressed = std::string{folly::StringPiece(iob->coalesce())};
    } catch (const std::bad_alloc&) {
        LOG_WARNING_CTX("Compression of config failed: No memory",
                        {"conn_id", cookie.getConnectionId()},
                        {"bucket", bucketname},
                        {"global", bucketname.empty()},
                        {"version", version.to_json()});
        return cb::engine_errc::no_memory;
    } catch (const std::exception& exception) {
        LOG_WARNING_CTX("Compression of config failed",
                        {"conn_id", cookie.getConnectionId()},
                        {"bucket", bucketname},
                        {"global", bucketname.empty()},
                        {"version", version.to_json()},
                        {"error", exception.what()});
        cookie.setErrorContext("Compression failed");
        return cb::engine_errc::failed;
    }

    auto configuration = std::make_shared<ClusterConfiguration::Configuration>(
            version, std::move(uncompressed), std::move(compressed));

    // Try to insert the new cluster configuration by using the provided
    // session token.
    cb::engine_errc status;
    auto state = Bucket::State::None;
    auto [rv, st] = BucketManager::instance().setClusterConfig(bucketname,
                                                               configuration);
    status = rv;
    state = st;

    if (status == cb::engine_errc::success) {
        return status;
    }

    if (status == cb::engine_errc::temporary_failure) {
        LOG_WARNING_CTX("Can't set config in this bucket state",
                        {"conn_id", cookie.getConnectionId()},
                        {"bucket", bucketname},
                        {"global", bucketname.empty()},
                        {"status", status},
                        {"state", state});
        return status;
    }

    LOG_WARNING_CTX("Failed to update config",
                    {"conn_id", cookie.getConnectionId()},
                    {"bucket", bucketname},
                    {"global", bucketname.empty()},
                    {"version", version.to_json()},
                    {"status", status});
    return status;
}

/// Push the configuration for the provided bucket to all clients
/// bound to the bucket and subscribe to notifications.
/// If an error occurs while pushing the configuration for the client
/// the client is shut down (as we might be out of sync protocol wise
/// on our send buffer)
///
/// Ideally we should have "preformatted" the message to send, but
/// due to the desire to do deduplication (in the case the map change
/// before we get around to push it to client X) we can't do that ;)
///
/// @param bucketname The name of the bucket to push notifications for
static void push_cluster_config(std::string_view bucketname) {
    // Iterate over all the connections and check if the connection is
    // associated with the provided bucket. iterate_all_connections
    // will inject a callback for each worker thread and run in the
    // connections' context while performing the callback
    iterate_all_connections([bucketname](Connection& connection) -> void {
        auto mode = connection.getClustermapChangeNotification();
        if (mode == ClustermapChangeNotification::None) {
            // The client hasn't asked to be notified
            return;
        }

        if (connection.getBucket().name != bucketname && !bucketname.empty()) {
            // this isn't the selected bucket, or we're not pushing the global
            // configuration
            return;
        }

        auto& bucket = bucketname.empty()
                               ? BucketManager::instance().getNoBucket()
                               : connection.getBucket();
        try {
            auto active = bucket.clusterConfiguration.maybeGetConfiguration(
                    bucket.type == BucketType::NoBucket
                            ? ClustermapVersion{}
                            : connection.getPushedClustermapRevno());
            if (!active) {
                // We've already pushed the latest version we've got
                return;
            }

            if (bucket.type != BucketType::NoBucket) {
                connection.setPushedClustermapRevno(active->version);
            }

            using namespace cb::mcbp;
            cb::mcbp::request::SetClusterConfigPayload version;
            version.setEpoch(active->version.getEpoch());
            version.setRevision(active->version.getRevno());
            size_t needed = sizeof(Request) + // packet header
                            sizeof(version) + // rev data in extdata
                            bucket.name.size(); // the name of the bucket
            if (mode == ClustermapChangeNotification::Full) {
                if (connection.supportsSnappyEverywhere()) {
                    needed += active->compressed.size(); // The actual payload
                } else {
                    needed += active->uncompressed.size(); // The actual payload
                }
            }

            std::string buffer;
            buffer.resize(needed);
            RequestBuilder builder(buffer);
            builder.setMagic(Magic::ServerRequest);
            builder.setOpcode(ServerOpcode::ClustermapChangeNotification);
            builder.setExtras(version.getBuffer());
            builder.setKey(bucket.name);
            if (mode == ClustermapChangeNotification::Full) {
                if (connection.supportsSnappyEverywhere()) {
                    builder.setDatatype(
                            cb::mcbp::Datatype{connection.getEnabledDatatypes(
                                    PROTOCOL_BINARY_DATATYPE_JSON |
                                    PROTOCOL_BINARY_DATATYPE_SNAPPY)});
                    builder.setValue(active->compressed);
                } else {
                    builder.setDatatype(
                            cb::mcbp::Datatype{connection.getEnabledDatatypes(
                                    PROTOCOL_BINARY_DATATYPE_JSON)});
                    builder.setValue(active->uncompressed);
                }
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

cb::engine_errc SetClusterConfigCommandContext::done() {
    LOG_INFO_CTX("Updated configuration",
                 {"conn_id", cookie.getConnectionId()},
                 {"bucket", bucketname},
                 {"global", bucketname.empty()},
                 {"version", version.to_json()});

    cookie.setCas(sessiontoken);
    cookie.sendResponse(cb::mcbp::Status::Success);

    // Schedule a "fire and forget" task to push to clients
    ExecutorPool::get()->schedule(
            std::make_shared<OneShotLimitedConcurrencyTask>(
                    TaskId::Core_PushClustermapTask,
                    "Push cluster configuration map",
                    [the_name_of_the_bucket = std::string{bucketname}]() {
                        try {
                            push_cluster_config(the_name_of_the_bucket);
                        } catch (const std::exception& exception) {
                            LOG_WARNING_CTX(
                                    "An error occurred while pushing cluster "
                                    "configurations",
                                    {"error", exception.what()});
                        }
                    },
                    ConcurrencySemaphores::instance().cccp_notification));
    return cb::engine_errc::success;
}
