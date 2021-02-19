/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2019 Couchbase, Inc.
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
 *
 *   This compilation unit (.h/.cc) contains the DurabilityMonitor internal
 *   (protected) structures. We do not put them in the DurabilityMonitor
 *   interface to avoid including them every time the DM header is included
 *   (as these structures are only used by the actual DM implementations, i.e.
 *   ADM and PDM).
 */
#pragma once

#include "active_durability_monitor.h"
#include "durability_monitor.h"
#include "item.h"
#include "monotonic.h"
#include "monotonic_queue.h"
#include "passive_durability_monitor.h"

#include <chrono>
#include <queue>
#include <unordered_map>
#include <unordered_set>

// An empty string is used to indicate an undefined node in a replication
// topology.
static const std::string UndefinedNode{};

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

    void setStatus(SyncWriteStatus status) {
        this->status = status;
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

/**
 * Represents a tracked durable write for use in the Active Durability Monitor.
 * Includes additional state over the base SyncWrite class for tracking number
 * of acks and the client to respond to etc.
 */
class DurabilityMonitor::ActiveSyncWrite : public DurabilityMonitor::SyncWrite {
public:
    /**
     * @param (optional) cookie The cookie representing the client connection.
     *     Necessary at Active for notifying the client at SyncWrite completion.
     * @param item The pending Prepare being wrapped
     * @param defaultTimeout If durability requirements in item do not specify
     *     a timeout, the timeout value to use for this SyncWrite.
     * @param (optional) firstChain The repl-chain that the write is tracked
     *     against.  Necessary at Active for verifying the SW Durability
     *     Requirements.
     * @param (optional) secondChain The second repl-chain that the write is
     *     tracked against. Necessary at Active for verifying the SW Durability
     *     Requirements.
     */
    ActiveSyncWrite(
            const void* cookie,
            queued_item item,
            std::chrono::milliseconds defaultTimeout,
            const ActiveDurabilityMonitor::ReplicationChain* firstChain,
            const ActiveDurabilityMonitor::ReplicationChain* secondChain);

    /**
     * Constructs a SyncWrite with an infinite timeout.
     *
     * @param (optional) cookie The cookie representing the client connection.
     *     Necessary at Active for notifying the client at SyncWrite completion.
     * @param item The pending Prepare being wrapped
     * @param (optional) firstChain The repl-chain that the write is tracked
     *     against.  Necessary at Active for verifying the SW Durability
     *     Requirements.
     * @param (optional) secondChain The second repl-chain that the write is
     *     tracked against. Necessary at Active for verifying the SW Durability
     *     Requirements.
     * @param InfiniteTimeout struct tag so it's clear from the caller an
     *        infinite timeout is being used.
     */
    struct InfiniteTimeout {};
    explicit ActiveSyncWrite(
            const void* cookie,
            queued_item item,
            const ActiveDurabilityMonitor::ReplicationChain* firstChain,
            const ActiveDurabilityMonitor::ReplicationChain* secondChain,
            InfiniteTimeout);

    /**
     * Move construct an ActiveSyncWrite from a normal (Passive) SyncWrite
     *
     * @param write The (Passive) SyncWrite
     */
    explicit ActiveSyncWrite(SyncWrite&& write);

    const void* getCookie() const;

    void clearCookie();

    std::chrono::steady_clock::time_point getStartTime() const;

    /**
     * Notify this SyncWrite that it has been ack'ed by node.
     *
     * @param node
     */
    void ack(const std::string& node);

    /**
     * @return true if the Durability Requirements are satisfied for this
     *     SyncWrite, false otherwise
     */
    bool isSatisfied() const;

    /**
     * Check if this SyncWrite is expired or not.
     *
     * @param asOf The time to be compared with this SW's expiry-time
     * @return true if this SW's expiry-time < asOf, false otherwise
     */
    bool isExpired(std::chrono::steady_clock::time_point asOf) const;

    /**
     * Reset the ack-state for this SyncWrite and set it up for the new
     * given topology. In general, checkDurabilityPossibleAndResetTopology
     * should be used to reset the topology for a SyncWrite.
     *
     * @param firstChain Reference to first chain
     * @param secondChain Pointer (may be null) to second chain
     */
    void resetTopology(
            const ActiveDurabilityMonitor::ReplicationChain& firstChain,
            const ActiveDurabilityMonitor::ReplicationChain* secondChain);

    /**
     * Reset the ack-state for this SyncWrite and set it up for the new given
     * topology if durability is possible for the new chains.
     *
     * @param firstChain Reference to first chain
     * @param secondChain Pointer (may be null) to second chain
     */
    void checkDurabilityPossibleAndResetTopology(
            const ActiveDurabilityMonitor::ReplicationChain& firstChain,
            const ActiveDurabilityMonitor::ReplicationChain* secondChain);

    const queued_item& getItem() const {
        return item;
    }

    /**
     * Performs sanity checks and initialise the replication chains
     * @param firstChain Pointer (may be null) to the first chain
     * @param secondChain Pointer (may be null) to second chain
     */
    void initialiseChains(
            const ActiveDurabilityMonitor::ReplicationChain* firstChain,
            const ActiveDurabilityMonitor::ReplicationChain* secondChain);

    /**
     * Reset the chains to nullptrs. They may be set accordingly when we move an
     * ActiveSyncWrite from trackedWrites to the resolvedQueue as we won't
     * update SyncWrites in the resolvedQueue when topologies change so we can
     * no longer trust the pointers.
     */
    void resetChains();

private:
    /**
     * Calculate the ackCount for this SyncWrite using the given chain.
     *
     * @param chain ReplicationChain to iterate on
     * @return The ackCount that we have for the given chain. Does not count the
     *         active
     */
    uint8_t getAckCountForNewChain(
            const ActiveDurabilityMonitor::ReplicationChain& chain);

    // Client cookie associated with this SyncWrite request, to be notified
    // when the SyncWrite completes.
    const void* cookie;

    /**
     * Holds all the information required for a SyncWrite to determine if it
     * is satisfied by a given chain. We can do all of this using the
     * ReplicationChain, but we store an ackCount as well as an optimization.
     */
    struct ChainStatus {
        explicit operator bool() const {
            return chainPtr;
        }

        void reset(const ActiveDurabilityMonitor::ReplicationChain* chainPtr,
                   uint8_t ackCount) {
            this->ackCount.reset(ackCount);
            this->chainPtr = chainPtr;
        }

        // Ack counter for the chain.
        // This optimization eliminates the need of scanning the positions map
        // in the ReplicationChain for verifying Durability Requirements.
        Monotonic<uint8_t> ackCount{0};

        // Pointer to the chain. Used to find out which node is the active and
        // what the majority value is.
        const ActiveDurabilityMonitor::ReplicationChain* chainPtr{nullptr};
    };

    ChainStatus firstChain;
    ChainStatus secondChain;

    // Used for enforcing the Durability Requirements Timeout. It is set
    // when this SyncWrite is added for tracking into the DurabilityMonitor.
    const std::optional<std::chrono::steady_clock::time_point> expiryTime = {};

    friend std::ostream& operator<<(std::ostream&, const ActiveSyncWrite&);
};

/**
 * Represents the tracked state of a node in topology.
 * Note that the lifetime of a Position is determined by the logic in
 * DurabilityMonitor.
 *
 * - it: Iterator that points to a position in the Container of tracked
 *         SyncWrites. This is an optimization: logically it points always
 * to the last SyncWrite acknowledged by the tracked node, so that we can
 * avoid any O(N) scan when updating the node state at seqno-ack received.
 * It may point to Container::end (e.g, when the pointed SyncWrite is the
 * last element in Container and it is removed).
 *
 * - lastWriteSeqno: Stores always the seqno of the last SyncWrite
 *         acknowledged by the tracked node, even when Position::it points
 * to Container::end. Used for validation at seqno-ack received and stats.
 *
 * - lastAckSeqno: Stores always the last seqno acknowledged by the tracked
 *         node. Used for validation at seqno-ack received and stats.
 */
template <typename Container>
struct DurabilityMonitor::Position {
    Position() = default;
    explicit Position(const typename Container::iterator& it) : it(it) {
    }
    typename Container::iterator it;
    // @todo: Consider using (strictly) Monotonic here. Weakly monotonic was
    // necessary when we tracked both memory and disk seqnos.
    // Now a Replica is not supposed to ack the same seqno twice.
    WeaklyMonotonic<int64_t, ThrowExceptionPolicy> lastWriteSeqno{0};
    WeaklyMonotonic<int64_t, ThrowExceptionPolicy> lastAckSeqno{0};
};

/**
 * Represents a VBucket Replication Chain in the ns_server meaning,
 * i.e. a list of active/replica nodes where the VBucket resides.
 */
struct ActiveDurabilityMonitor::ReplicationChain {
    /**
     * @param nodes The list of active/replica nodes in the ns_server format
     *         {active, replica1, replica2, replica3}
     *     Replica node(s) (but not active) can be logically undefined if:
     *     a) auto-failover has occurred but the cluster hasn't yet been
     *         rebalanced. As such the old replica (which is now the active)
     *         hasn't been replaced yet.
     *     b) Bucket has had the replica count increased but not yet rebalanced.
     *         To assign the correct replicas. An undefined replica is
     *         represented by an empty node name (""s).
     *
     * @param name Name of chain (used for stats and exception logging)
     * @param nodes The names of the nodes in this chain
     * @param initPos The initial position for tracking iterators in chain
     * @param maxAllowedReplicas Should SyncWrites be blocked
     *        (isDurabilityPossible() return false) if there are more than N
     *        replicas configured?
     *        Workaround for known issue with failover / rollback - see
     *        MB-34453 / MB-34150.
     */
    ReplicationChain(const DurabilityMonitor::ReplicationChainName name,
                     const std::vector<std::string>& nodes,
                     const Container::iterator& initPos,
                     size_t maxAllowedReplicas);

    size_t size() const;

    bool isDurabilityPossible() const;

    // Check if the given node has acked at least the given seqno
    bool hasAcked(const std::string& node, int64_t bySeqno) const;

    // Index of node Positions. The key is the node id.
    // A Position embeds the seqno-state of the tracked node.
    std::unordered_map<std::string, Position<Container>> positions;

    // Majority in the arithmetic definition:
    //     chain-size / 2 + 1
    const uint8_t majority;

    const std::string active;

    // Workaround for MB-34150 (tracked via MB-34453): Block SyncWrites if
    // there are more than this many replicas in the chain as we
    // cannot guarantee no dataloss in a particular failover+rollback scenario.
    // (Exposed as a member variable to allow tests to override it so we can
    // defend the bulk of functionality which does work).
    const size_t maxAllowedReplicas;

    // Name of the chain
    const DurabilityMonitor::ReplicationChainName name;
};

// Used to track information necessary for the PassiveDM to correctly ACK
// at the end of snapshots
struct SnapshotEndInfo {
    int64_t seqno;
    CheckpointType type;
};

/**
 * Compares two SnapshotEndInfos by seqno.
 *
 * Only provides a partial ordering; SnapshotEndInfos with the same seqno
 * but different checkpoint type would be arbitrarily ordered if using this
 * comparator
 */
bool operator>(const SnapshotEndInfo& a, const SnapshotEndInfo& b);
std::string to_string(const SnapshotEndInfo& snapshotEndInfo);

/*
 * This class embeds the state of an ADM. It has been designed for being
 * wrapped by a folly::Synchronized<T>, which manages the read/write
 * concurrent access to the T instance.
 * Note: all members are public as accessed directly only by ADM, this is
 * a protected struct. Avoiding direct access by ADM would require
 * re-implementing most of the ADM functions into ADM::State and exposing
 * them on the ADM::State public interface.
 *
 * This resides in this file (and not active_durability_monitor.cc as one might
 * expect) because the PassiveDM ctor implemenation needs access to it be able
 * to construct an PassiveDM from the ActiveDM's state.
 */
struct ActiveDurabilityMonitor::State {
    /**
     * @param adm The owning ActiveDurabilityMonitor
     */
    explicit State(const ActiveDurabilityMonitor& adm);

    /**
     * Create a replication chain. Not static as we require an iterator from
     * trackedWrites.
     *
     * @param name Name of chain (used for stats and exception logging)
     * @param chain Unique ptr to the chain
     */
    std::unique_ptr<ReplicationChain> makeChain(
            const DurabilityMonitor::ReplicationChainName name,
            const nlohmann::json& chain);

    /**
     * Set the replication topology from the given json. If the new topology
     * makes durability impossible then this function will abort any in-flight
     * SyncWrites by enqueuing them in the ResolvedQueue toAbort.
     *
     * @param topology Json topology
     * @param toComplete Reference to the resolvedQueue so that we can abort
     *        any SyncWrites for which durability is no longer possible.
     */
    void setReplicationTopology(const nlohmann::json& topology,
                                ResolvedQueue& toComplete);

    /**
     * Add a new SyncWrite
     *
     * @param cookie Connection to notify on completion
     * @param item The prepare
     */
    void addSyncWrite(const void* cookie, queued_item item);

    /**
     * Returns the next position for a node iterator.
     *
     * @param node
     * @return the iterator to the next position for the given node. Returns
     *         trackedWrites.end() if the node is not found.
     */
    Container::iterator getNodeNext(const std::string& node);

    /**
     * Advance a node tracking to the next Position in the tracked
     * Container. Note that a Position tracks a node in terms of both:
     * - iterator to a SyncWrite in the tracked Container
     * - seqno of the last SyncWrite ack'ed by the node
     *
     * @param node the node to advance
     * @return an iterator to the new position (tracked SyncWrite) of the
     *         given node.
     * @throws std::logic_error if the node is not found
     */
    Container::iterator advanceNodePosition(const std::string& node);

    /**
     * This function updates the tracking with the last seqno ack'ed by
     * node.
     *
     * Does nothing if the node is not found. This may be the case
     * during a rebalance when a new replica is acking sync writes but we do
     * not yet have a second chain because ns_server is waiting for
     * persistence to allow sync writes to be transferred the the replica
     * asynchronously. When the new replica catches up to the active,
     * ns_server will give us a second chain.
     *
     * @param node
     * @param seqno New ack seqno
     */
    void updateNodeAck(const std::string& node, int64_t seqno);

    /**
     * Updates a node memory/disk tracking as driven by the new ack-seqno.
     *
     * @param node The node that ack'ed the given seqno
     * @param ackSeqno
     * @param [out] toCommit Container which has all SyncWrites to be Commited
     * appended to it.
     */
    void processSeqnoAck(const std::string& node,
                         int64_t ackSeqno,
                         ResolvedQueue& toCommit);

    /**
     * Removes expired Prepares from tracking which are eligable to be timed
     * out (and Aborted).
     *
     * @param asOf The time to be compared with tracked-SWs' expiry-time
     * @param [out] the ResolvedQueue to enqueue the expired Prepares onto.
     */
    void removeExpired(std::chrono::steady_clock::time_point asOf,
                       ResolvedQueue& expired);

    /// @returns the name of the active node. Assumes the first chain is valid.
    const std::string& getActive() const;

    int64_t getNodeWriteSeqno(const std::string& node) const;

    int64_t getNodeAckSeqno(const std::string& node) const;

    /**
     * Remove the given SyncWrte from tracking.
     *
     * @param it The iterator to the SyncWrite to be removed
     * @param status The SyncWriteStatus to set the SyncWrite to as we remove it
     * @return the removed SyncWrite.
     */
    ActiveSyncWrite removeSyncWrite(Container::iterator it,
                                    SyncWriteStatus status);

    /**
     * Logically 'moves' forward the High Prepared Seqno to the last
     * locally-satisfied Prepare. In other terms, the function moves the HPS
     * to before the current durability-fence.
     *
     * Details.
     *
     * In terms of Durability Requirements, Prepares at Active can be
     * locally-satisfied:
     * (1) as soon as the they are queued into the PDM, if Level Majority
     * (2) when they are persisted, if Level PersistToMajority or
     *     MajorityAndPersistOnMaster
     *
     * We call the first non-satisfied PersistToMajority or
     * MajorityAndPersistOnMaster Prepare the "durability-fence".
     * All Prepares /before/ the durability-fence are locally-satisfied.
     *
     * This functions's internal logic performs (2) first by moving the HPS
     * up to the latest persisted Prepare (i.e., the durability-fence) and
     * then (1) by moving to the HPS to the last Prepare /before/ the new
     * durability-fence (note that after step (2) the durability-fence has
     * implicitly moved as well).
     *
     * Note that in the ActiveDM the HPS is implemented as the Active
     * tracking in FirstChain. So, differently from the PassiveDM, here we
     * do not have a dedicated HPS iterator.
     *
     * @param completed The ResolvedQueue to enqueue all prepares satisfied
     *        (ready for commit) by the HPS update
     */
    void updateHighPreparedSeqno(ResolvedQueue& completed);

    void updateHighCompletedSeqno();

    /// Debug - print a textual description of this object to stderr.
    void dump() const;

protected:
    /**
     * Set up the newFirstChain correctly if we previously had no topology.
     *
     * When a replica is promoted to active, the trackedWrites are moved from
     * the PDM to the ADM. This ADM will have a null topology and the active
     * node iterator will not exist. When we move from a null topology to a
     * topology, we need to correct the HPS iterator to ensure that the HPS is
     * correct post topology change. The HPS iterator is set to the
     * corresponding SyncWrite in trackedWrites. If we have just received a Disk
     * snapshot as PDM and highPreparedSeqno is not equal to anything in
     * trackedWrites then it will be set to the highest seqno less than the
     * highPreparedSeqno.
     *
     * @param newFirstChain our new firstChain
     */
    void transitionFromNullTopology(ReplicationChain& newFirstChain);

    /**
     * Move the Positions (iterators and write/ack seqnos) from the old chains
     * to the new chains. Copies between two chains too so that a node that
     * previously existed int he second chain and now only exists in the first
     * will have the correct iterators and seqnos (this is a normal swap
     * rebalance scenario).
     *
     * @param firstChain old first chain
     * @param newFirstChain new first chain
     * @param secondChain old second chain
     * @param newSecondChain new second chain
     */
    void copyChainPositions(ReplicationChain* firstChain,
                            ReplicationChain& newFirstChain,
                            ReplicationChain* secondChain,
                            ReplicationChain* newSecondChain);

    /**
     * Copy the Positions from the given old chain to the given new chain.
     */
    void copyChainPositionsInner(ReplicationChain& oldChain,
                                 ReplicationChain& newChain);

    /**
     * A topology change may make durable writes impossible in the case of a
     * failover. We abort the in-flight SyncWrites to allow the client to retry
     * them to provide an earlier response. This retry would then be met with a
     * durability impossible error if the cluster had not yet been healed.
     *
     * Note, we cannot abort any in-flight SyncWrites with an infinite
     * timeout as these must be committed. They will eventually be committed
     * when the cluster is healed or the number of replicas is dropped such that
     * they are satisfied. These SyncWrites may exist due to either warmup or
     * a Passive->Active transition.
     *
     * @param newFirstChain Chain against which we check if durability is
     *        possible
     * @param newSecondChain Chain against which we check if durability is
     *        possible
     * @param[out] toAbort Container which has all SyncWrites to abort appended
     *             to it
     */
    void abortNoLongerPossibleSyncWrites(ReplicationChain& newFirstChain,
                                         ReplicationChain* newSecondChain,
                                         ResolvedQueue& toAbort);

    /**
     * Perform the manual ack (from the map of queuedSeqnoAcks) that is
     * required at rebalance for the given chain
     *
     * @param chain Chain for which we should manually ack nodes
     * @param[out] toCommit Container which has all SyncWrites to be committed
     *             appended to it.
     */
    void performQueuedAckForChain(const ReplicationChain& chain,
                                  ResolvedQueue& toCommit);

    /**
     * Queue a seqno ack to be processed after the topology has been updated.
     *
     * Tracks acks received either before any topology has been received,
     * or from nodes not currently in the topology (i.e., a new replica node
     * acking prior to setTopology)
     */
    void queueSeqnoAck(const std::string& node, int64_t seqno);

    /**
     * A topology change may trigger a commit due to number of replicas
     * changing. Generally we commit by moving the HPS or receiving a seqno ack
     * but we cannot call the typical updateHPS function at topology change.
     * This function iterates on trackedWrites committing anything that
     * needs commit. We may also have SyncWrites in trackedWrites that were
     * completed by a previous PDM but are needed to correctly set the HPS when
     * we receive the replication topology from ns_server; these SyncWrites
     * should be removed from trackedWrites at this point.
     *
     * @param [out] toCommit Container which has all SyncWrites to be committed
     *              appended to it.
     */
    void cleanUpTrackedWritesPostTopologyChange(ResolvedQueue& toCommit);

private:
    /**
     * Advance the current Position (iterator and seqno).
     *
     * @param pos the current Position of the node
     * @param node the node to advance (used to update the SyncWrite if
     *        acking)
     * @param shouldAck should we call SyncWrite->ack() on this node?
     *        Optional as we want to avoid acking a SyncWrite twice if a
     *        node exists in both the first and second chain.
     */
    void advanceAndAckForPosition(Position<Container>& pos,
                                  const std::string& node,
                                  bool shouldAck);

    /**
     * throw exception with the following error string:
     *   "ActiveDurabilityMonitor::State::<thrower>:<error> vb:x"
     *
     * @param thrower a string for who is throwing, typically __func__
     * @param error a string containing the error and any useful data
     * @throws exception
     */
    template <class exception>
    [[noreturn]] void throwException(const std::string& thrower,
                                     const std::string& error) const;

public:
    /// The container of pending Prepares.
    Container trackedWrites;

    /**
     * @TODO Soon firstChain will be optional for warmup - update comment
     * Our replication topology. firstChain is a requirement, secondChain is
     * optional and only required for rebalance. It will be a nullptr if we
     * do not have a second replication chain.
     */
    std::unique_ptr<ReplicationChain> firstChain;
    std::unique_ptr<ReplicationChain> secondChain;

    // Always stores the seqno of the last SyncWrite added for tracking.
    // Useful for sanity checks, necessary because the tracked container
    // can by emptied by Commit/Abort.
    Monotonic<int64_t, ThrowExceptionPolicy> lastTrackedSeqno{0};

    // Stores the last committed seqno. Throws as this could be the result of an
    // out of order completion that would break durability.
    Monotonic<int64_t, ThrowExceptionPolicy> lastCommittedSeqno{0};

    // Stores the last aborted seqno. Throws as this could be the result of an
    // out of order completion that would break durability.
    Monotonic<int64_t, ThrowExceptionPolicy> lastAbortedSeqno{0};

    // Stores the highPreparedSeqno. Throws as an incorrect value may mean we
    // are doing durability wrong.
    WeaklyMonotonic<int64_t, ThrowExceptionPolicy> highPreparedSeqno{0};

    // Stores the highCompletedSeqno. Does not throw as this is a stat used for
    // debugging.
    Monotonic<int64_t> highCompletedSeqno{0};

    // Cumulative count of accepted (tracked) SyncWrites.
    size_t totalAccepted = 0;
    // Cumulative count of Committed SyncWrites.
    size_t totalCommitted = 0;
    // Cumulative count of Aborted SyncWrites.
    size_t totalAborted = 0;

    // The durability timeout value to use for SyncWrites which haven't
    // specified an explicit timeout.
    // @todo-durability: Allow this to be configurable.
    static constexpr std::chrono::milliseconds defaultTimeout =
            std::chrono::seconds(30);

    const ActiveDurabilityMonitor& adm;

    // Map of node to seqno value for seqno acks that we have seen but
    // do not exist in the current replication topology. They may be
    // required to manually ack for a new node if we receive an ack before
    // ns_server sends us a new replication topology. Throws because a replica
    // should never ack backwards.
    std::unordered_map<std::string, Monotonic<int64_t, ThrowExceptionPolicy>>
            queuedSeqnoAcks;

    friend std::ostream& operator<<(std::ostream& os, const State& s) {
        os << "#trackedWrites:" << s.trackedWrites.size()
           << " highPreparedSeqno:" << s.highPreparedSeqno
           << " highCompletedSeqno:" << s.highCompletedSeqno
           << " lastTrackedSeqno:" << s.lastTrackedSeqno
           << " lastCommittedSeqno:" << s.lastCommittedSeqno
           << " lastAbortedSeqno:" << s.lastAbortedSeqno << " trackedWrites:["
           << "\n";
        for (const auto& w : s.trackedWrites) {
            os << "    " << w << "\n";
        }
        os << "]\n";
        os << "firstChain: ";
        if (s.firstChain) {
            chainToOstream(os, *s.firstChain, s.trackedWrites.end());
        } else {
            os << "<null>";
        }
        os << "\nsecondChain: ";
        if (s.secondChain) {
            chainToOstream(os, *s.secondChain, s.trackedWrites.end());
        } else {
            os << "<null>";
        }
        os << "\n";
        return os;
    }
};

/*
 * This class embeds the state of a PDM. It has been designed for being
 * wrapped by a folly::Synchronized<T>, which manages the read/write
 * concurrent access to the T instance.
 * Note: all members are public as accessed directly only by PDM, this is
 * a protected struct. Avoiding direct access by PDM would require
 * re-implementing most of the PDM functions into PDM::State and exposing
 * them on the PDM::State public interface.
 *
 * This resides in this file (and not passive_durability_monitor.cc as one might
 * expect) because the ActiveDM ctor implemenation needs access to it be able
 * to construct an ActiveDM from the PassiveDM's state.
 */
struct PassiveDurabilityMonitor::State {
    /**
     * @param pdm The owning PassiveDurabilityMonitor
     */
    explicit State(const PassiveDurabilityMonitor& pdm);

    /**
     * Returns the next position for a given Container::iterator.
     *
     * @param it The iterator
     * @return the next position in Container
     */
    typename Container::iterator getIteratorNext(
            const typename Container::iterator& it);

    /**
     * Logically 'moves' forward the High Prepared Seqno to the last
     * locally-satisfied Prepare. In other terms, the function moves the HPS
     * to before the current durability-fence.
     *
     * Details.
     *
     * In terms of Durability Requirements, Prepares at Replica can be
     * locally-satisfied:
     * (1) as soon as the they are queued into the PDM, if Level Majority or
     *     MajorityAndPersistOnMaster
     * (2) when they are persisted, if Level PersistToMajority
     *
     * We call the first non-satisfied PersistToMajority Prepare the
     * "durability-fence". All Prepares /before/ the durability-fence are
     * locally-satisfied and can be ack'ed back to the Active.
     *
     * This functions's internal logic performs (2) first by moving the HPS
     * up to the latest persisted Prepare (i.e., the durability-fence) and
     * then (1) by moving to the HPS to the last Prepare /before/ the new
     * durability-fence (note that after step (2) the durability-fence has
     * implicitly moved as well).
     */
    void updateHighPreparedSeqno();

    /**
     * Check if there are Prepares eligible for removal, and remove them if
     * any. A Prepare is eligible for removal if:
     *
     * 1) it is completed (Committed or Aborted)
     * 2) it is locally-satisfied
     *
     * In terms of PassiveDM internal structures, the above means that a
     * Prepare is eligible for removal if both the HighCompletedSeqno and
     * the HighPreparedSeqno have covered it.
     *
     * Thus, this function is called every time the HCS and the HPS are
     * updated.
     */
    void checkForAndRemovePrepares();

    /**
     * Check for and drop any collections from droppedCollections that we can
     */
    void checkForAndRemoveDroppedCollections();

    /**
     * Erase the SyncWrite at the given iterator after fixing up the iterators
     * for the HCS and HPS values (if they point to the element to be erased)
     *
     * @return trackedWrites.erase(...) result
     */
    Container::iterator safeEraseSyncWrite(Container::iterator toErase);

    /// The container of pending Prepares.
    Container trackedWrites;

    // The seqno of the last Prepare satisfied locally. I.e.:
    //     - the Prepare has been queued into the PDM, if Level Majority
    //         or MajorityAndPersistToMaster
    //     - the Prepare has been persisted locally, if Level
    //         PersistToMajority
    Position<Container> highPreparedSeqno;

    // Queue of SnapshotEndInfos for snapshots which have been received entirely
    // by the (owning) replica/pending VBucket, and the type of the checkpoint.
    // Used for implementing the correct move-logic of High Prepared Seqno.
    // Must be pushed to at snapshot-end received on PassiveStream.
    MonotonicQueue<SnapshotEndInfo> receivedSnapshotEnds;

    // Cumulative count of accepted (tracked) SyncWrites.
    size_t totalAccepted = 0;
    // Cumulative count of Committed SyncWrites.
    size_t totalCommitted = 0;
    // Cumulative count of Aborted SyncWrites.
    size_t totalAborted = 0;

    // Points to the last Prepare that has been completed (Committed or
    // Aborted). Together with the HPS Position, it is used for implementing
    // the correct Prepare remove-logic in PassiveDM.
    Position<Container> highCompletedSeqno;

    // Map of collections to their end seqnos that have been dropped and may
    // have outstanding prepares. Required to allow us to skip over and clean up
    // prepares for dropped collections in trackedWrites without acquiring the
    // collection VB manifest handle which would cause a lock order inversion.
    std::unordered_map<CollectionID, int64_t> droppedCollections;

    const PassiveDurabilityMonitor& pdm;
};

template <typename Container>
std::string to_string(const DurabilityMonitor::Position<Container>& pos,
                      typename Container::const_iterator trackedWritesEnd) {
    std::stringstream ss;
    ss << "{lastAck:" << pos.lastAckSeqno << " lastWrite:" << pos.lastWriteSeqno
       << " it:";
    if (pos.it == trackedWritesEnd) {
        ss << "<end>";
    } else {
        ss << " @" << &*pos.it << " seqno:" << pos.it->getBySeqno();
    }
    ss << "}";
    return ss.str();
}
