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
 * Command context for executing SyncFusionLogstore requests in a AUXIO
 * bg-thread and avoiding blocking IO in frontend threads.
 */
class SyncFusionLogstoreCommandContext : public SteppableCommandContext {
public:
    explicit SyncFusionLogstoreCommandContext(Cookie& cookie);

protected:
    cb::engine_errc step() override;
    bool completed = false;
    const Vbid vbid;
};
