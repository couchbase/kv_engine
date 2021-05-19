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
#include "cccp_notification_task.h"

#include "buckets.h"
#include "connection.h"
#include "log_macros.h"
#include "memcached.h"
#include "server_event.h"

#include <mcbp/protocol/framebuilder.h>
#include <memory>
#include <string>

CccpNotificationTask::CccpNotificationTask(unsigned int bucketIndex,
                                           const ClustermapVersion& version)
    : Task(), bucketIndex(bucketIndex), version(version) {
    // Bump a reference so the bucket can't be deleted while we're in the
    // middle of pushing configurations (we're already bound to the bucket)
    all_buckets[bucketIndex].clients++;
}

CccpNotificationTask::~CccpNotificationTask() {
    disconnect_bucket(all_buckets[bucketIndex], nullptr);
}

class CccpPushNotificationServerEvent : public ServerEvent {
public:
    explicit CccpPushNotificationServerEvent(unsigned int index)
        : bucketIndex(index) {
    }

    std::string getDescription() const override {
        return "CccpPushNotificationServerEvent";
    }

    bool execute(Connection& connection) override {
        if (bucketIndex != 0 && bucketIndex != connection.getBucketIndex()) {
            // This isn't the global configuration or the selected bucket
            // so we shouldn't push the configuration
            return true;
        }

        std::unique_ptr<ClusterConfiguration::Configuration> active;

        if (bucketIndex == 0) {
            active = all_buckets[0].clusterConfiguration.maybeGetConfiguration(
                    {});
        } else {
            auto& bucket = connection.getBucket();
            auto pushed = connection.getPushedClustermapRevno();
            active = bucket.clusterConfiguration.maybeGetConfiguration(pushed);
        }

        if (!active) {
            // We've already pushed the latest version we've got
            return true;
        }

        std::string name;
        {
            std::lock_guard<std::mutex> guard(all_buckets[bucketIndex].mutex);
            if (all_buckets[bucketIndex].state == Bucket::State::Ready) {
                name = all_buckets[bucketIndex].name;
            } else {
                // The bucket is no longer online
                return true;
            }
        }

        if (bucketIndex == 0) {
            LOG_INFO("{}: Sending global Cluster map revision: {}",
                     connection.getId(),
                     active->version);
        } else {
            connection.setPushedClustermapRevno(active->version);
            LOG_INFO("{}: Sending Cluster map for bucket:{} revision: {}",
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
        builder.setKey(
                {reinterpret_cast<const uint8_t*>(name.data()), name.size()});
        builder.setValue(
                {reinterpret_cast<const uint8_t*>(active->config.data()),
                 active->config.size()});

        // Inject our packet into the stream!
        connection.copyToOutputStream(builder.getFrame()->getFrame());
        return true;
    }

    const unsigned int bucketIndex;
};

Task::Status CccpNotificationTask::execute() {
    if (bucketIndex == 0) {
        LOG_INFO("Pushing new global cluster config - revision:[{}]", version);
    } else {
        LOG_INFO("Pushing new cluster config for bucket:[{}] revision:[{}]",
                 all_buckets[bucketIndex].name,
                 version);
    }

    auto index = bucketIndex;

    // This feels a bit dirty, but the problem is that when we had
    // the task being created we did hold the FrontEndThread mutex
    // when we locked the task in order to schedule it.
    // Now we want to iterate over all of the connections, and in
    // order to do that we need to lock the libevent thread so that
    // we can get exclusive access to the connection objects for that
    // thread.
    // No one is using this task so we can safely release the lock
    getMutex().unlock();
    try {
        iterate_all_connections([index](Connection& connection) -> void {
            if (!connection.isClustermapChangeNotificationSupported()) {
                // The client hasn't asked to be notified
                return;
            }

            connection.enqueueServerEvent(
                    std::make_unique<CccpPushNotificationServerEvent>(index));
            connection.signalIfIdle();
        });
    } catch (const std::exception& e) {
        LOG_WARNING("CccpNotificationTask::execute: received exception: {}",
                    e.what());
    }
    getMutex().lock();
    return Status::Finished;
}
