/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2017 Couchbase, Inc
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

class EPVBucket;

/*
 * This is a NONIO task called as part of VB deletion.  The task is responsible
 * for clearing all the VBucket's pending operations and for deleting the
 * VBucket (via a smart pointer).
 *
 * This task is designed to be invoked only when the VBucket has no owners.
 */
class VBucketMemoryDeletionTask : public GlobalTask {
public:
    /**
     * @param engine required for GlobalTask construction
     * @param vbucket the vbucket object to delete
     */
    VBucketMemoryDeletionTask(EventuallyPersistentEngine& eng,
                              VBucket* vbucket,
                              TaskId tid = TaskId::VBucketMemoryDeletionTask);

    ~VBucketMemoryDeletionTask() override;

    std::string getDescription() override;

    std::chrono::microseconds maxExpectedDuration() override;

    bool run() override;

protected:
    /**
     * Call vbucket->notifyAllPendingConnsFailed and optionally perform
     * notifyIOComplete
     *
     * @pararm notifyIfCookieSet set to true if the function should perform
     *         notifyIOComplete on vbucket->getDeletingCookie()
     */
    void notifyAllPendingConnsFailed(bool notifyIfCookieSet);

    /**
     * The vbucket we are deleting is stored in a unique_ptr for RAII deletion
     * once this task is finished and itself deleted, the VBucket will be
     * deleted.
     */
    std::unique_ptr<VBucket> vbucket;
    std::string description;
};

/*
 * This is an AUXIO task called as part of EPVBucket deletion.  The task is
 * responsible for clearing all the VBucket's pending operations and for
 * clearing the VBucket's hash table and removing the disk file.
 *
 * This task is designed to be invoked only when the EPVBucket has no owners.
 */
class VBucketMemoryAndDiskDeletionTask : public VBucketMemoryDeletionTask {
public:
    /**
     * This task will as part of construction increase the vbucket's disk
     * revision so that the delete can remove the file without new
     * instances of the same vbucket writing to the file.
     *
     * @param engine requird for GlobalTask construction
     * @param shard the KVShard to use for deletion
     * @param vbucket the Eventually Persistent vbucket object to delete
     */
    VBucketMemoryAndDiskDeletionTask(EventuallyPersistentEngine& engine,
                                     KVShard& shard,
                                     EPVBucket* vbucket);

    bool run() override;

protected:
    KVShard& shard;
    uint64_t vbDeleteRevision;
};
