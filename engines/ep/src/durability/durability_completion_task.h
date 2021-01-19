/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2019 Couchbase, Inc
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
#include "vb_ready_queue.h"
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

    std::string getDescription() override {
        return "DurabilityCompletionTask";
    }

    std::chrono::microseconds maxExpectedDuration() override {
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
