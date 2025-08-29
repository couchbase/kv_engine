/*
 *     Copyright 2025-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#pragma once

#include "steppable_command_context.h"

#include <memcached/engine.h>

/**
 * Mounts a vbucket using the paths in the request and responds with the DEK ids
 * used by the snapshot.
 *
 * The command is need for Fusion to gather DEK ids before issuing SetVbState.
 */
class MountFusionVbucketCommandContext : public SteppableCommandContext {
public:
    enum class State { Mount, SendResponse, Done };

    explicit MountFusionVbucketCommandContext(Cookie& cookie);

protected:
    cb::engine_errc step() override;

    cb::engine_errc mount();

    cb::engine_errc sendResponse();

    VBucketSnapshotSource source;
    std::vector<std::string> paths;
    std::string response;
    State state;
};
