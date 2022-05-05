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

#include "durability_monitor.h"
#include "ep_types.h"
#include "storeddockey_fwd.h"
#include "utilities/testing_hook.h"

#include <folly/SynchronizedPtr.h>
#include <optional>

#include <vector>

class ActiveDurabilityMonitor;
class CollectionID;
struct DocKey;
class RollbackResult;
struct vbucket_state;
class VBucket;

/*
 * The DurabilityMonitor for Replica VBuckets.
 *
 * The PassiveDurabilityMonitor (PDM) is responsible for ack'ing received
 * Prepares back to the Active. The logic in the PDM ensures that Prepares are
 * ack'ed in seqno-order, which is fundamental for achieving:
 * - In-Order Commit at Active
 * - Consistency at failure scenarios
 */
class PassiveDurabilityMonitor : public DurabilityMonitor {
public:
    // Container type used for State::trackedWrites
    using Container = std::list<SyncWrite>;

    explicit PassiveDurabilityMonitor(VBucket& vb);

    /**
     * Construct a PassiveDM for the given vBucket, with the specified hps/hcs
     * @param vb VBucket which owns this Durability Monitor.
     * @param highPreparedSeqno seqno to use as the initial highPreparedSeqno
     * @param highCompletedSeqno seqno to use as the initial highCompletedSeqno
     */
    PassiveDurabilityMonitor(VBucket& vb,
                             int64_t highPreparedSeqno,
                             int64_t highCompletedSeqno);

    /**
     * Construct a PassiveDM for the given vBucket using pre-existing state.
     *
     * This constructor is used by warmup where the HPS/HCS exist in the vbucket
     * state and warmup locates all persisted prepares.
     *
     * This constructor is used during state changes where an
     * ActiveDurabilityMonitor must handover any outstanding prepares when an
     * active switches to replica.
     *
     * @param vb VBucket which owns this Durability Monitor.
     * @param highPreparedSeqno seqno to use as the initial highPreparedSeqno
     * @param highCompletedSeqno seqno to use as the initial highCompletedSeqno
     * @param outstandingPrepares In-flight prepares which the DM should take
     *        responsibility for.
     *        These must be ordered by ascending seqno, otherwise
     *        std::invalid_argument will be thrown.
     */
    PassiveDurabilityMonitor(VBucket& vb,
                             int64_t highPreparedSeqno,
                             int64_t highCompletedSeqno,
                             std::vector<queued_item>&& outstandingPrepares);

    /**
     * Construct a PassiveDM for the given vBucket using pre-existing state.
     *
     * This constructor is used when we transition state from ADM to PDM.
     *
     * This constructor is used during state changes where an
     * ActiveDurabilityMonitor must handover any outstanding prepares when an
     * active switches to replica.
     *
     * @param vb VBucket which owns this Durability Monitor.
     * @param adm The ActiveDM to be converted
     */
    PassiveDurabilityMonitor(VBucket& vb, ActiveDurabilityMonitor&& adm);

    ~PassiveDurabilityMonitor() override;

    void addStats(const AddStatFn& addStat,
                  const CookieIface* cookie) const override;

    int64_t getHighPreparedSeqno() const override;

    int64_t getHighCompletedSeqno() const override;

    /**
     * Add a pending Prepare for tracking into the PDM.
     *
     * @param item the queued_item
     * @param overwritingPrepareSeqno should we overwrite an existing prepare if
     *                                one exists with this seqno?
     */
    void addSyncWrite(queued_item item,
                      std::optional<int64_t> overwritingPrepareSeqno = {});

    /**
     * The reason a SyncWrite has been completed.
     *
     */
    enum class Resolution : uint8_t {
        /// Commit: Has met the durability requirements and is "sucessful"
        Commit,
        /// Abort: Failed to meet the durability requirements (within the
        /// timeout)
        Abort,
    };

    /**
     * Complete the given Prepare, i.e. remove it from tracking.
     *
     * @param key The key of the Prepare to be removed
     * @param res The type of resolution, Commit/Abort
     * @param prepareSeqno The seqno of the prepare that should be removed (if
     * known)
     */
    void completeSyncWrite(const StoredDocKey& key,
                           Resolution res,
                           std::optional<uint64_t> prepareSeqno);

    /**
     * Erase the SyncWrite with the given key and seqno from the DM.
     *
     * Does not move the HCS or HPS values as this would be incorrect (neither
     * can move forward if we don't prepare or complete something, neither can
     * move backwards at all).
     *
     * Moves iterators of the HPS and HCS /backwards/ to the first valid
     * position.
     *
     * @param key Key to drop
     * @param seqno Expected seqno of the SyncWrite we are dropping
     */
    void eraseSyncWrite(const DocKey& key, int64_t seqno);

    void notifyDroppedCollection(CollectionID cid, int64_t seqno);

    size_t getNumDroppedCollections() const;

    static std::string to_string(Resolution res);

    size_t getNumTracked() const override;

    size_t getNumAccepted() const override;
    size_t getNumCommitted() const override;
    size_t getNumAborted() const override;

    /**
     * Notify this PDM that the snapshot-end mutation has been received for the
     * owning VBucket.
     * The snapshot-end seqno is used for the correct implementation of the HPS
     * move-logic.
     *
     * @param vbStateLock the lock holder for the vBucket state lock
     * @param snapEnd The snapshot-end seqno
     */
    void notifySnapshotEndReceived(folly::SharedMutex::ReadHolder& vbStateLock,
                                   uint64_t snapEnd);

    /**
     * Notify this PDM that some persistence has happened. Attempts to update
     * the HPS and ack back to the active.
     */
    void notifyLocalPersistence(
            folly::SharedMutex::ReadHolder& vbStateLock) override;

    int64_t getHighestTrackedSeqno() const override;

    void dump() const override;

    /**
     * Test only: Hook which if non-empty is called from
     * notifySnapshotEndReceived()
     */
    TestingHook<> notifySnapEndSeqnoAckPreProcessHook;

protected:
    /**
     * Store the seqno ack that we should now send to the consumer. Overwrites
     * any outstanding ack not yet sent if the new value is greater.
     *
     * @param prevHps determines if we should send an ack or not
     * @param newHps new hps to ack
     */
    void storeSeqnoAck(int64_t prevHps, int64_t newHps);

    /**
     * Send, if we need to, a seqno ack to the active node.
     */
    void sendSeqnoAck();

    void toOStream(std::ostream& os) const override;
    /**
     * throw exception with the following error string:
     *   "<thrower>:<error> vb:x"
     *
     * @param thrower a string for who is throwing, typically __FUNCTION__
     * @param error a string containing the error and any useful data
     * @throws exception
     */
    template <class exception>
    [[noreturn]] void throwException(const std::string& thrower,
                                     const std::string& error) const;

    // The VBucket owning this DurabilityMonitor instance
    VBucket& vb;

    /// PassiveDM state. Guarded by folly::Synchronized to manage concurrent
    /// access. Uses unique_ptr for pimpl.
    struct State;
    folly::SynchronizedPtr<std::unique_ptr<State>> state;

    /// Outstanding seqno ack to send to the active. 0 if no ack outstanding
    folly::Synchronized<int64_t> seqnoToAck{0};

    // Necessary for implementing ADM(PDM&&)
    friend class ActiveDurabilityMonitor;

    friend std::ostream& operator<<(
            std::ostream& os, const PassiveDurabilityMonitor::State& state);
};
