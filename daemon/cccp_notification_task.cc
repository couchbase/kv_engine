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

#include <mcbp/protocol/framebuilder.h>
#include <memory>
#include <string>

CccpNotificationTask::CccpNotificationTask(Bucket& bucket_, int revision_)
    : Task(), bucket(bucket_), revision(revision_) {
    // Bump a reference so the bucket can't be deleted while we're in the
    // middle of pushing configurations (we're already bound to the bucket)
    bucket.clients++;
}

CccpNotificationTask::~CccpNotificationTask() {
    disconnect_bucket(bucket, nullptr);
}

Task::Status CccpNotificationTask::execute() {
    if (bucket.type == BucketType::NoBucket) {
        LOG_INFO("Pushing new global cluster config - revision:[{}]", revision);
    } else {
        LOG_INFO("Pushing new cluster config for bucket:[{}] revision:[{}]",
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

            // The extras contains the cluster revision number as an uint32_t
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
        LOG_WARNING("CccpNotificationTask::execute: received exception: {}",
                    e.what());
    }
    return Status::Finished;
}
