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

#include "sync_write.h"

#include "vbucket.h"

/**
 * The DurabilityMonitor (DDM) for Dead vBuckets.
 *
 * The DDM is created when we transition a vBucket to dead. It exists as an
 * ADM or PDM does not have suitable behaviour for a vBucket in the dead state.
 * A DDM acts as a placeholder and stores only the state that we would move when
 * we transition from active to replica or vice-versa.
 *
 * It currently performs no action.
 */
class DeadDurabilityMonitor : public DurabilityMonitor {
public:
    /**
     * Ctor for transitioning from some other type of DM to a DDM.
     * @param vb The owning vBucket
     * @param oldDM The old DM
     */
    DeadDurabilityMonitor(VBucket& vb, DurabilityMonitor&& oldDM);

    /**
     * Ctor for creating a new DM with no previous state.
     * @param vb The owning vBucket
     */
    DeadDurabilityMonitor(VBucket& vb) : vb(vb) {
    }

    void addStats(const AddStatFn& addStat,
                  const CookieIface* cookie) const override;

    int64_t getHighPreparedSeqno() const override {
        return highPreparedSeqno;
    }

    int64_t getHighCompletedSeqno() const override {
        return highCompletedSeqno;
    }

    int64_t getHighestTrackedSeqno() const override;

    size_t getNumTracked() const override {
        return trackedWrites.size();
    }

    size_t getNumAccepted() const override {
        // DDM should not accept anything and values are not transferred
        return 0;
    }

    size_t getNumCommitted() const override {
        // DDM should not commit anything and values are not transferred
        return 0;
    }

    size_t getNumAborted() const override {
        // DDM should not abort anything and values are not transferred
        return 0;
    }

    void notifyLocalPersistence() override {
        // No work required here
    }

    void dump() const override;

    std::list<SyncWrite> getTrackedWrites() const override;

    std::optional<int64_t> getLastConsistentSeqno() const {
        return lastConsistentSeqno;
    }

protected:
    void toOStream(std::ostream& os) const override;

    VBucket& vb;

    int64_t highPreparedSeqno = 0;
    int64_t highCompletedSeqno = 0;

    using Container = std::list<SyncWrite>;
    Container trackedWrites;

    // lastConsistentSeqno is taken when we transition to DDM. If we are
    // transitioning from active to dead (ADM to DDM) then it is the
    // (in-memory) high seqno at that point. If we are transitioning from
    // replica or pending to dead (PDM to DDM) then it is the last received
    // snapshot end (may have been the high seqno of the vBucket if it was
    // previously active and has not yet received another snapshot). This is
    // used later when we transition to PDM to allow a newly created PDM to ack
    // back up to that seqno.
    std::optional<int64_t> lastConsistentSeqno = {};
};
