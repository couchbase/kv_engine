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
 *
 * Tests in this suite test lazy checkpoint removal.
 */
class CheckpointRemoverTest : public STParameterizedBucketTest {
public:
    void SetUp() override;

    /// How is memory expected to be recovered in the below tests?
    enum class MemRecoveryMode {
        CursorDrop,
        ItemExpelWithCursor,
        ItemExpelWithoutCursor,
    };

    /**
     * Helper function to the expelButNoCursorDrop and
     * notExpelButCursorDrop tests. It adds a cursor to a checkpoint that
     * been configured to more easily allow the triggering of memory
     * recovery.  It then adds items to a checkpoint, flushes those items
     * to disk.  Then depending on the mode parameter:
     * - CursorDrop: Cursor left at start of checkpoint, expect memory to
     *   be recovered by dropping it.
     * - ItemExpelWithCursor: Cursor is moved past 90% of the items added -
     *   so ItemExpel can occur.
     * - ItemExpelWithoutCursor: Cursor is not added; relying solely on
     *   item expel.
     * It then invokes the run method of CheckpointMemRecoveryTask, which
     * first attempts to recover memory by expelling followed, if necessary,
     * by cursor dropping, and the subsequent removal of closed unreferenced
     * checkpoints.
     * @param mode  indicates the test variant.
     * test should be moved forward.
     */
    void testExpellingOccursBeforeCursorDropping(MemRecoveryMode mode);

    /**
     * Helper function to test the getBytesToFree method of
     * CheckpointMemRecoveryTask.
     * @param triggeredByHWM true if remover is triggered by bucket HWM
     *          false if remover triggered by the checkpoint upper mark.
     */
    void testGetBytesToFree(KVBucket& bucket,
                            std::shared_ptr<CheckpointMemRecoveryTask> remover,
                            bool triggeredByHWM);
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
     * Construct a cursor, and call getItemsForCursor.
     *
     * @param name
     * @param startBySeqno
     */
    std::vector<queued_item> getItemsWithCursor(const std::string& name,
                                                uint64_t startBySeqno,
                                                bool expectBackfill = false);

    /**
     * Creates a new open checkpoint and moves the persistence cursors into it.
     * That removes the old checkpoints.
     *
     * @param manager The CM to operate on
     */
    void createCheckpointAndEnsureOldRemoved(CheckpointManager& manager);
};
