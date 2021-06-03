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
#pragma once

#include "vb_ready_queue.h"
#include <executor/globaltask.h>
#include <memcached/vbucket.h>

/*
 * This task is used to complete (commit or abort) all SyncWrites which have
 * been resolved by each vbucket's ActiveDM.
 *
 * This is done in a separate task to reduce the amount of work done on
 * the thread which actually detected the SyncWrite was resolved - typically
 * the front-end DCP threads when a DCP_SEQNO_ACK is processed.
 * Given that we SEQNO_ACK at the end of Snapshot, A single SEQNO_ACK could
 * result in committing multiple SyncWrites, and Committing one SyncWrite is
 * similar to a normal front-end Set operation, we want to move this to a
 * background task.
 *
 * Additionally, by doing this in a background task it simplifies lock
 * management, for example we avoid lock inversions with earlier locks acquired
 * during dcpSeqnoAck when attemping to later call notifySeqnoAvailable when
 * this was done on the original thread.
 */
class DurabilityCompletionTask : public GlobalTask {
public:
    explicit DurabilityCompletionTask(EventuallyPersistentEngine& engine);

    bool run() override;

    std::string getDescription() const override {
        return "DurabilityCompletionTask";
    }

    std::chrono::microseconds maxExpectedDuration() const override {
        // Task shouldn't run much longer than maxChunkDuration; given we yield
        // after that duration - however _could_ exceed a bit given we check
        // the duration on each vBucket. As such add a 2x margin of error.
        return std::chrono::duration_cast<std::chrono::microseconds>(
                2 * maxChunkDuration);
    }

    /**
     * Notifies the task that the given vBucket has SyncWrite(s) ready to
     * be completed.
     * If the given vBucket isn't already pending, then will wake up the task
     * for it to run.
     */
    void notifySyncWritesToComplete(Vbid vbid);

private:
    /**
     * Queue of unique vBuckets to process.
     */
    VBReadyQueue queue;

    /**
     * Flag which is used to check if a wakeup has already been scheduled for
     * this task.
     */
    std::atomic<bool> wakeUpScheduled{false};

    /// Maximum duration this task should execute for before yielding back to
    /// the ExecutorPool (to allow other tasks to run).
    static const std::chrono::steady_clock::duration maxChunkDuration;
};
