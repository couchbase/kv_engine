/*
 *     Copyright 2023-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#pragma once
#include "daemon/cluster_config.h"
#include "steppable_command_context.h"

#include <daemon/memcached.h>
#include <mcbp/protocol/opcode.h>
#include <mcbp/protocol/request.h>

/**
 * SetClusterConfigCommandContext implements the SetClusterConfig
 * command.
 *
 * It receives the Cluster map from ns_server and spawns a task to create
 * a compressed version of the cluster configuration. The compressed and
 * uncompressed version is then stored in the bucket before another task
 * gets kicked off iterating over all of the connections trying to push
 * a notification to all connections subscribing to notifications.
 */
class SetClusterConfigCommandContext : public SteppableCommandContext {
public:
    explicit SetClusterConfigCommandContext(Cookie& cookie);
    cb::engine_errc step() override;

protected:
    /// The callback to run in the dedicated task to compress the input
    /// and use the provided session token to replace the configuration
    /// (and possibly create the ConfigOnly bucket)
    cb::engine_errc doSetClusterConfig();

    /// Build up the reply to the client
    cb::engine_errc done();

    /// The name of the bucket we'll operate on (empty == global config)
    const std::string bucketname;

    /// The session token provided by the client
    const uint64_t sessiontoken;

    /// The version we're about to set
    const ClustermapVersion version;

    /// The uncompressed version of the configuration the client is going to set
    std::string uncompressed;

    /// The two different states for the state machine. We're starting in
    /// ScheduleTask which will schedule a task to be run and set the state to
    /// Done which is responsible for sending the reply back to the client
    /// and kick off the fire-and-forget task to push cluster configuration
    enum class State { ScheduleTask, Done };

    /// The state in the state machine for the command context
    State state = State::ScheduleTask;
};
