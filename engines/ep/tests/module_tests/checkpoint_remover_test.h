/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2018 Couchbase, Inc
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

/*
 * Unit tests for the checkpoint remover and associated functions.
 */

#pragma once

#include "checkpoint.h"
#include "ep_bucket.h"
#include "evp_store_single_threaded_test.h"
#include "evp_store_test.h"

class CheckpointManager;

/*
 * A subclass of KVBucketTest which uses a fake ExecutorPool,
 * which will not spawn ExecutorThreads and hence not run any tasks
 * automatically in the background. All tasks must be manually run().
 */
class CheckpointRemoverTest : public SingleThreadedKVBucketTest {
public:
    void SetUp() override;

    /**
     * Get the maximum number of items allowed in a checkpoint for the given
     * vBucket
     */
    size_t getMaxCheckpointItems(VBucket& vb);
};

/**
 * Test fixture for single-threaded tests on EPBucket.
 */
class CheckpointRemoverEPTest : public CheckpointRemoverTest {
protected:
    EPBucket& getEPBucket() {
        return dynamic_cast<EPBucket&>(*store);
    }

    /**
     * Helper function to the expelButNoCursorDrop and
     * notExpelButCursorDrop tests. It adds a cursor to a checkpoint that
     * been configured to more easily allow the triggering of memory
     * recovery.  It then adds items to a checkpoint, flushes those items
     * to disk.  Then depending on whether the moveCursor parameter is
     * true will move the cursor past 90% of the items added.  It then
     * invokes the run method of ClosedUnrefCheckpointRemoverTask, which
     * first attempts to recover memory by expelling followed, if necessary,
     * by cursor dropping, and the subsequent removal of closed unreferenced
     * checkpoints.
     * @param moveCursor  indicates whether the cursor created in the
     * test should be moved forward.
     */
    void testExpelingOccursBeforeCursorDropping(bool moveCursor);

    /**
     * Construct a cursor, and call getNextItemsForCursor.
     *
     * @param name
     * @param startBySeqno
     */
    std::vector<queued_item> getItemsWithCursor(const std::string& name,
                                                uint64_t startBySeqno,
                                                bool expectBackfill = false);
};
