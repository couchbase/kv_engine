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
#include "memcached/engine_error.h"
#include "utilities/testing_hook.h"

#include <fmt/ostream.h>
#include <folly/SynchronizedPtr.h>
#include <memcached/storeddockey_fwd.h>
#include <nlohmann/json_fwd.hpp>

#include <unordered_set>

struct DocKeyView;
class EPStats;
class PassiveDurabilityMonitor;
struct vbucket_state;
class VBucket;

/*
 * The DurabilityMonitor for Active VBuckets.
 *
 * The ActiveDurabilityMonitor (ADM) drives the completion (commit/abort) of
 * SyncWrites requests. To do that, the ADM tracks the pending SyncWrites queued
 * at Active and the ACKs sent by Replicas to verify if the Durability
 * Requirements are satisfied for the tracked mutations.
 *
 * Lifecycle
 * =========
 *
 * All SyncWrites progess through the following lifecycle:
 *
 *     Prepared -> Resolved -> Completed
 *
 * - Prepared: SyncWrite has been accepted into the DurabilityMonitor,
 *   and is awaiting sufficient nodes to acknowledge it within the timeout
 *   period.
 *
 * - Resolved: SyncWrite has either:
 *   a) Met the durability requirements (sufficient nodes have ack'd it)
 *      and should be Committed, or
 *   b) It has exceeded the timeout and should be Aborted. SyncWrite is moved
 *      from trackedWrites into resolvedQueue.
 *
 * - Completed: SyncWrite resolution (Commit / Abort) has been applied to the
 *   VBucket, and hence the SyncWrite has reached the end of it's lifecycle.
 *
 * Implementation
 * ==============
 *
 * In-flight SyncWrites are held in an ordered container (linked list) of
 * trackedWrites, with new SyncWrites being appended to the back of
 * trackedWrites.
 *
 * Each node (active and replica) involved in Sync Replication for this vBucket
 * maintains an iterator (Position) into trackedWrites, to track what seqno that
 * node has acknowledged up to. Once sufficient nodes have ack'd then
 * the SyncWrite can be Committed.
 *
 * Tracked Writes iterator semantics
 * =================================
 *
 * A node's iterator is either:
 * a) !end() - Points to last SyncWrite which has been ACK'd by this node.
 * b) Container::end() - this indicates the iterator is invalid - either
 *    the node has not ACK'd any seqnos (initial state), or the last SyncWrite
 *    which was ACK'd has been removed *and* there is no previous SyncWrite
 *    to point the iterator to.
 *
 * Iterator usage:
 * A) To add a SyncWrite:
 *     1. Add SyncWrite added to back of trackedWrites. No iterator update
 *        needed.
 *
 * B) To remove a SyncWrite:
 *     1. For each node iterator:
 *     2. If iter pointing at the SyncWrite to be removed, then move iter to the
 *        previous SyncWrite:
 *        - If iter == begin(); then move iter to end().
 *        - Else move iter to prev(iter).
 *
 *  C) To acknowledge a seqno:
 *      1. Examine iterator's successor item:
 *          - If iter == end(), then successor is begin (iterator currently
 *            invalid).
 *          - Else successor is next(iter).
 *      2. If successor is less than or equal to ack'd seqno, then mark `*iter`
 *         SyncWrite as acknowledged, set iter == successor.
 *      3. Repeat from step (1).
 */
class ActiveDurabilityMonitor : public DurabilityMonitor {
public:
    struct ReplicationChain;
    // Container type used for State::trackedWrites
    using Element = DurabilityMonitor::ActiveSyncWrite;
    using Container = DurabilityMonitorTrackedWrites<Element>;

    /**
     * Construct an ActiveDM for the given vBucket.
     * @param stats EPStats object for the associated Bucket.
     * @param vb VBucket which owns this Durability Monitor.
     * @param nextExpiryChanged Object to use for timing out SyncWrites.
     */
    ActiveDurabilityMonitor(EPStats& stats,
                            VBucket& vb,
                            std::unique_ptr<EventDrivenDurabilityTimeoutIface>
                                    nextExpiryChanged);

    /**
     * Construct an ActiveDM for the given vBucket, with the specified
     * outstanding prepares as the initial state of the tracked SyncWrites. Used
     * by warmup to restore the state as it was before restart.
     * @param stats EPStats object for the associated Bucket.
     * @param vb VBucket which owns this Durability Monitor.
     * @param vbs reference to the vbucket_state found at warmup
     * @param nextExpiryChanged Object to use for timing out SyncWrites.
     * @param outstandingPrepares In-flight prepares which the DM should take
     *        responsibility for.
     *        These must be ordered by ascending seqno, otherwise
     *        std::invalid_argument will be thrown.
     */
    ActiveDurabilityMonitor(EPStats& stats,
                            VBucket& vb,
                            const vbucket_state& vbs,
                            std::unique_ptr<EventDrivenDurabilityTimeoutIface>
                                    nextExpiryChanged,
                            std::vector<queued_item>&& outstandingPrepares);

    /**
     * Construct an ActiveDM by converting the given DM.
     * All the (in-flight) tracked Prepares in the old DM are retained.
     *
     * @param stats EPStats object for the associated Bucket.
     * @param vb VBucket which owns this Durability Monitor.
     * @param dm The DM to be converted
     * @param nextExpiryChanged Object to use for timing out SyncWrites.
     */
    ActiveDurabilityMonitor(EPStats& stats,
                            VBucket& vb,
                            DurabilityMonitor&& dm,
                            std::unique_ptr<EventDrivenDurabilityTimeoutIface>
                                    nextExpiryChanged);

    ~ActiveDurabilityMonitor() override;

    /**
     * Sets the Replication Topology.
     *
     * @param topology The topology encoded as nlohmann::json array of (max 2)
     *     replication chains. Each replication chain is itself a
     *     nlohmann::json array of nodes representing the chain.
     * @throw std::invalid_argument
     */
    void setReplicationTopology(const nlohmann::json& topology);

    /**
     * Set the commit strategy. If the new commit strategy makes durability
     * impossible then this function will abort any in-flight SyncWrites. If the
     * new commit strategy allows some writes to be committed, they wil also be
     * processed as part of this call.
     *
     * @param newStrategy The requirements for a durable write to be
     * comitted.
     */
    void setAndProcessCommitStrategy(CommitStrategy newStrategy);

    /// @returns the high_prepared_seqno.
    int64_t getHighPreparedSeqno() const override;

    /**
     * @return the High Completed Seqno, the last Committed or Aborted Prepare
     */
    int64_t getHighCompletedSeqno() const override;

    /// @returns highest tracked seqno
    int64_t getHighestTrackedSeqno() const override;

    /**
     * @return true if the replication topology allows Majority being reached,
     *     false otherwise
     */
    bool isDurabilityPossible() const;

    /**
     * Start tracking a new SyncWrite.
     * Expected to be called by VBucket::add/update/delete after a new SyncWrite
     * has been inserted into the HashTable and enqueued into the
     * CheckpointManager.
     *
     * @param cookie Optional client cookie which will be notified the SyncWrite
     *        completes.
     * @param item the queued_item
     * @throw std::logic_error if the replication-chain is not set
     */
    void addSyncWrite(CookieIface* cookie, queued_item item);

    /**
     * Expected to be called by memcached at receiving a DCP_SEQNO_ACK packet.
     *
     * @param replica The replica that sent the ACK
     * @param diskSeqno The ack'ed prepared seqno.
     * @return cb::engine_errc::success if the operation succeeds, an error code
     * otherwise
     * @throw std::logic_error if the received seqno is unexpected
     */
    cb::engine_errc seqnoAckReceived(const std::string& replica,
                                     int64_t preparedSeqno);

    /**
     * Enforce timeout for the expired SyncWrites in the tracked list.
     *
     * @param asOf The time to be compared with tracked-SWs' expiry-time
     * @throw std::logic_error
     */
    void processTimeout(cb::time::steady_clock::time_point asOf);

    /**
     * Get (and clear) the non-null cookies for all in-flight SyncWrites.
     * (Null cookies - for example originating from SyncWrites loaded during
     * warmup - are not returned). The reason for clearing the cookies is
     * to avoid a double notification on the cookie (which is illegal),
     * so the caller <u>must</u> notify these cookies.
     */
    std::vector<CookieIface*> getCookiesForInFlightSyncWrites();

    /**
     * Prepare for a transition away from active by moving every prepare in the
     * resolvedQueue to trackedWrites then returning the cookies to be responded
     * to with ambiguous.
     *
     * @return The cookies of all in-flight SyncWrites. Return value of
     *         getCookiesForInFlightSyncWrites.
     */
    std::vector<CookieIface*> prepareTransitionAwayFromActive();

    void notifyLocalPersistence() override;

    /**
     * Output DurabiltyMonitor stats.
     *
     * @param addStat the callback to memcached
     * @param cookie
     */
    void addStats(const AddStatFn& addStat, CookieIface& cookie) const override;

    size_t getNumTracked() const override;

    size_t getNumAccepted() const override;
    size_t getNumCommitted() const override;
    size_t getNumCommittedNotDurable() const override;
    size_t getNumAborted() const override;

    /**
     * Get the memory used by this object. The memory used only includes the
     * size of all Item objects (Item::getSize) that are referenced as "tracked
     * writes".
     * @return memory used in bytes (see above for detail as to what is tracked)
     */
    size_t getTotalMemoryUsed() const override;

    /**
     * @return the size of FirstChain
     */
    uint8_t getFirstChainSize() const;

    /**
     * @return the size of SecondChain
     */
    uint8_t getSecondChainSize() const;

    /**
     * @return the FirstChain Majority
     */
    uint8_t getFirstChainMajority() const;

    /**
     * @return the SecondChain Majority
     */
    uint8_t getSecondChainMajority() const;

    /**
     * Returns the seqno of the SyncWrites currently pointed by the
     * internal tracking for Node. E.g., if we have a tracked SyncWrite list
     * {s:1, s:2} and we receive a SeqnoAck{2}, then the internal tracking will
     * be at s:2, which is what this function returns.
     * Note that this may differ from Replica AckSeqno. Using the same example,
     * if we receive SeqnoAck{3} then the internal tracking will still point to
     * s:2, which is what this function will return again.
     *
     * @param node
     * @return the seqno of the SyncWrite currently pointed by the internal
     *     tracking for Node.
     */
    int64_t getNodeWriteSeqno(const std::string& node) const;

    /**
     * Returns the last seqno ack'ed by Node.
     * Note that this may differ from Node write-seqno.
     *
     * @param node
     * @return the last seqno ack'ed by Node
     */
    int64_t getNodeAckSeqno(const std::string& node) const;

    /**
     * Test only.
     *
     * @return the set of seqnos tracked by this DurabilityMonitor
     */
    std::unordered_set<int64_t> getTrackedSeqnos() const;

    /**
     * Check if we can commit any SyncWrites and commit them.
     */
    void checkForCommit();

    /**
     * We track acks for unknown nodes as they may precede a topology change
     * that could cause a SyncWrite to timeout. We only receive these acks via
     * DCP so we can remove any "unknown" ack for a given node when we close the
     * ActiveStream serving it.
     *
     * @param node Node for which we wish to remove the unknown ack
     */
    void removedQueuedAck(const std::string& node);

    /**
     * For all items in the completedSWQueue, call VBucket::commit /
     * VBucket::abort as appropriate, then remove the item from the queue.
     */
    void processCompletedSyncWriteQueue(VBucketStateLockRef vbStateLock);

    /**
     * Remove all of the prepares from the resolvedQueue and put them back into
     * trackedWrites.
     *
     * Why?
     *
     * If we are about to transition from active to non-active then we need to
     * ensure that the DM state is consistent with the HashTable as we use it
     * to create a PDM. If we were to process the queue then this node would get
     * out of step with the new active and need to rollback (or potentially have
     * two different items with the same seqno).
     */
    void unresolveCompletedSyncWriteQueue();

    /**
     * @return all of the currently tracked keys
     */
    std::vector<StoredDocKey> getTrackedKeys() const;

    std::list<SyncWrite> getTrackedWrites() const override;

    /// Debug - print a textual description of this object to stderr.
    void dump() const override;

    /// Prints the given ReplicationChain to the stream.
    static void chainToOstream(std::ostream& os,
                               const ReplicationChain& rc,
                               Container::const_iterator trackedWritesEnd);

    /**
     * Erase the SyncWrite with the given key and seqno from the DM.
     *
     * Does not move the HCS or HPS values as this would be incorrect (neither
     * can move forward if we don't prepare or complete something, neither can
     * move backwards at all).
     *
     * Moves iterators of acks /backwards/ to the first valid position.
     *
     * @param key Key to drop
     * @param seqno Expected seqno of the SyncWrite we are dropping
     */
    void eraseSyncWrite(const DocKeyView& key, int64_t seqno);

protected:
    void toOStream(std::ostream& os) const override;

    /**
     * throw exception with the following error string:
     *   "ActiveDurabilityMonitor::<thrower>:<error> vb:x"
     *
     * @param thrower a string for who is throwing, typically __func__
     * @param error a string containing the error and any useful data
     * @throws exception
     */
    template <class exception>
    [[noreturn]] void throwException(const std::string& thrower,
                                     const std::string& error) const;

    /**
     * Commit the given SyncWrite.
     *
     * @param vbStateLock A lock on the state of the VBucket.
     * @param sw The SyncWrite to commit
     */
    void commit(VBucketStateLockRef vbStateLock, const ActiveSyncWrite& sw);

    /**
     * Abort the given SyncWrite.
     *
     * @param vbStateLock A lock on the state of the VBucket.
     * @param sw The SyncWrite to abort
     */
    void abort(VBucketStateLockRef vbStateLock, const ActiveSyncWrite& sw);

    /**
     * Test only: Hook which if non-empty is called from seqnoAckReceived()
     * after calling State::processSeqnoAck.
     */
    TestingHook<> seqnoAckReceivedPostProcessHook;

    /**
     * Validate the given json replication chain checking if it's an array, not
     * too large etc.
     *
     * @param chain json replication chain
     * @param chainName name printed in exceptions
     * @throws std::invalid_argument if the chain is invalid
     */
    static void validateChain(
            const nlohmann::json& chain,
            DurabilityMonitor::ReplicationChainName chainName);

    /**
     * Output DurabilityMonitor stats for the given chain
     *
     * @param addStat the callback to memcached
     * @param cookie
     * @param vbid raw vb id printed in stats
     * @param chainName name of the chain, printed in stats
     * @param chain reference to the chain to output stats for
     */
    void addStatsForChain(const AddStatFn& addStat,
                          CookieIface& cookie,
                          const ReplicationChain& chain) const;

    /**
     * Checks if the resolvedQueue contains any SyncWrites awaiting completion,
     * and if so notifies the VBucket.
     */
    void checkForResolvedSyncWrites();

    // The stats object for the owning Bucket
    EPStats& stats;

    // The VBucket owning this DurabilityMonitor instance
    VBucket& vb;

    // The allocator which state will use (must be constructed before state)
    MemoryTrackingAllocator<Element> trackedWritesAllocator;

    /// Bulk of ActiveDM state. Guarded by folly::Synchronized to manage
    /// concurrent access. Uses unique_ptr for pimpl.
    struct State;
    folly::SynchronizedPtr<std::unique_ptr<State>> state;

    class ResolvedQueue;

    /**
     * The queue of SyncWrites which have been resolved (ready to be Committed
     * or Aborted) by the Durability Monitor and hence need to be applied to the
     * VBucket.
     *
     * Uses unique_ptr for pimpl.
     */
    std::unique_ptr<ResolvedQueue> resolvedQueue;

    // Maximum number of replicas which can be specified in topology.
    static const size_t maxReplicas = 3;

    // Necessary for implementing PDM(ADM&&)
    friend class PassiveDurabilityMonitor;

    // @todo: Try to remove this, currenlty necessary for testing wipeTracked()
    friend class ActiveDurabilityMonitorTest;
};

template <>
struct fmt::formatter<ActiveDurabilityMonitor> : ostream_formatter {};
