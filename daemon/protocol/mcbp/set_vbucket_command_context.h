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

/**
 * Command context for executing SetVbucket commands
 */
class SetVbucketCommandContext : public SteppableCommandContext {
public:
    explicit SetVbucketCommandContext(Cookie& cookie);

protected:
    cb::engine_errc step() override;
    const vbucket_state_t state;
    const Vbid vbid;
    nlohmann::json meta;
};
