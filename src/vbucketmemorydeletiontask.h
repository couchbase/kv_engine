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

#pragma once

#include "globaltask.h"
#include "vbucket.h"
/*
 * This is a NONIO task called as part of VB deletion.  The task is responsible
 * for clearing all the VBucket's pending operations and for clearing the
 * VBucket's hash table.
 */
class VBucketMemoryDeletionTask : public GlobalTask {
public:
    VBucketMemoryDeletionTask(EventuallyPersistentEngine& eng,
                              RCPtr<VBucket>& vb,
                              double delay);

    std::string getDescription();

    bool run();

private:
    EventuallyPersistentEngine& e;
    RCPtr<VBucket> vbucket;
};
