/*
 *     Copyright 2022-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#pragma once

#include "durability_monitor.h"

#include "item.h"
#include "storeddockey_fwd.h"

/**
 * The status of an in-flight SyncWrite
 */
enum class SyncWriteStatus {
    // Still waiting for enough acks to commit or to timeout.
    Pending = 0,

    // Should be committed, enough nodes have acked. Should not exist in
    // trackedWrites in this state.
    ToCommit,

    // Should be aborted. Should not exist in trackedWrites in this state.
    ToAbort,

    // A replica receiving a disk snapshot or a snapshot with a persist level
    // prepare may not remove the SyncWrite object from trackedWrites until
    // it has been persisted. This SyncWrite has been Completed but may still
    // exist in trackedWrites.
    Completed,
};

std::string to_string(SyncWriteStatus status);

/**
 * Represents a tracked durable write. It is mainly a wrapper around a pending
 * Prepare item. This SyncWrite object is used to track a durable write on
 * non-active nodes.
 */
class DurabilityMonitor::SyncWrite {
public:
    explicit SyncWrite(queued_item item);

    const StoredDocKey& getKey() const;

    int64_t getBySeqno() const;

    cb::durability::Requirements getDurabilityReqs() const;

    void setStatus(SyncWriteStatus newStatus) {
        status = newStatus;
    }

    SyncWriteStatus getStatus() const {
        return status;
    }

    /**
     * @return true if this SyncWrite has been logically completed
     */
    bool isCompleted() const {
        return status == SyncWriteStatus::Completed;
    }

protected:
    // An Item stores all the info that the DurabilityMonitor needs:
    // - seqno
    // - Durability Requirements
    // Note that queued_item is a ref-counted object, so the copy in the
    // CheckpointManager can be safely removed.
    const queued_item item;

    /// The time point the SyncWrite was added to the DurabilityMonitor.
    /// Used for statistics (track how long SyncWrites take to complete).
    const std::chrono::steady_clock::time_point startTime;

    SyncWriteStatus status = SyncWriteStatus::Pending;

    friend std::ostream& operator<<(std::ostream&, const SyncWrite&);
};