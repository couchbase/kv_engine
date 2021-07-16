/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2019-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "durability_completion_task.h"

#include "ep_engine.h"
#include "vbucket.h"
#include <executor/executorpool.h>

#include <climits>

using namespace std::chrono_literals;

DurabilityCompletionTask::DurabilityCompletionTask(
        EventuallyPersistentEngine& engine)
    : VBNotifiableTask(engine,
                       TaskId::DurabilityCompletionTask,
                       25ms /* maxChunkDuration */) {
}

void DurabilityCompletionTask::visitVBucket(VBucket& vb) {
    vb.processResolvedSyncWrites();
}

void DurabilityCompletionTask::notifySyncWritesToComplete(Vbid vbid) {
    notify(vbid);
}
