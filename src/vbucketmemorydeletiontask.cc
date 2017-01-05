/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2016 Couchbase, Inc
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

#include "vbucketmemorydeletiontask.h"

#include <phosphor/phosphor.h>

#include <sstream>

VBucketMemoryDeletionTask::VBucketMemoryDeletionTask(
        EventuallyPersistentEngine& eng, RCPtr<VBucket>& vb, double delay)
    : GlobalTask(&eng, TaskId::VBucketMemoryDeletionTask, delay, true),
      e(eng),
      vbucket(vb) {
    if (!vb) {
        throw std::invalid_argument(
                "VBucketMemoryDeletionTask: vb to delete cannot be null");
    }
    description = "Removing (dead) vb:" + std::to_string(vbucket->getId()) +
                  " from memory";
}

std::string VBucketMemoryDeletionTask::getDescription() {
    return description;
}

bool VBucketMemoryDeletionTask::run() {
    TRACE_EVENT("ep-engine/task", "VBucketMemoryDeletionTask",
                vbucket->getId());
    vbucket->notifyAllPendingConnsFailed(e);
    vbucket->ht.clear();
    vbucket.reset();
    return false;
}
