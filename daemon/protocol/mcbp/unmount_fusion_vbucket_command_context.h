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
#include "background_thread_command_context.h"

/**
 * Unmounts a previously mounted vbucket in an AUXIO background thread.
 */
class UnmountFusionVbucketCommandContext
    : public BackgroundThreadCommandContext {
public:
    UnmountFusionVbucketCommandContext(Cookie& cookie);

protected:
    cb::engine_errc execute() override;
};
