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

#include "ep_task.h"
#include "vb_ready_queue.h"
#include <memcached/vbucket.h>
#include <vb_notifiable_task.h>

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
 *
 * Multiple instances of this task exist per bucket (see
 * KVBucket::createAndScheduleDurabilityCompletionTasks). Each vBucket is
 * assigned to one of them for as long as it exists on this node, so
 * completion of a single bucket's SyncWrites can scale beyond a single thread
 * while still completing each vBucket's SyncWrites in-order.
 */
class DurabilityCompletionTask : public VBNotifiableTask {
public:
    /**
     * @param engine the engine this task is associated with
     * @param id identifier of this task within the bucket's set of
     *        DurabilityCompletionTasks - used to distinguish them in
     *        logs / task stats.
     */
    DurabilityCompletionTask(EventuallyPersistentEngine& engine, size_t id);

    void visitVBucket(VBucket& vb) override;

    std::string getDescription() const override {
        return "DurabilityCompletionTask:" + std::to_string(id);
    }

    /**
     * Notifies the task that the given vBucket has SyncWrite(s) ready to
     * be completed.
     * If the given vBucket isn't already pending, then will wake up the task
     * for it to run.
     */
    void notifySyncWritesToComplete(Vbid vbid);

private:
    /// Identifier of this task within the bucket's set of tasks.
    const size_t id;
};
