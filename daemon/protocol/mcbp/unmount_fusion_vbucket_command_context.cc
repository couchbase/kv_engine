/*
 *     Copyright 2025-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "unmount_fusion_vbucket_command_context.h"

#include <daemon/concurrency_semaphores.h>
#include <daemon/connection.h>
#include <executor/globaltask.h>

UnmountFusionVbucketCommandContext::UnmountFusionVbucketCommandContext(
        Cookie& cookie)
    : BackgroundThreadCommandContext(
              cookie,
              TaskId::Core_UnmountFusionVBucketTask,
              fmt::format("UnmountFusionVbucket:{}",
                          cookie.getRequest().getVBucket()),
              ConcurrencySemaphores::instance().fusion_management) {
}

cb::engine_errc UnmountFusionVbucketCommandContext::execute() {
    auto& engine = cookie.getConnection().getBucketEngine();
    return engine.unmountVBucket(cookie.getRequest().getVBucket());
}
