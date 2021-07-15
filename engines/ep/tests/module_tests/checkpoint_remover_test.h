/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2018-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
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
    void testExpellingOccursBeforeCursorDropping(bool moveCursor);

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
