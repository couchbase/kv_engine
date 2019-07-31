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

#include "durability_monitor.h"
#include "item.h"
#include "monotonic.h"
#include "monotonic_queue.h"
#include "passive_durability_monitor.h"

#include <chrono>
#include <queue>
#include <unordered_map>
#include <unordered_set>

class PassiveDurabilityMonitor;

// An empty string is used to indicate an undefined node in a replication
// topology.
static const std::string UndefinedNode{};

/**
 * Represents a tracked SyncWrite. It is mainly a wrapper around a pending
 * Prepare item.
 */
class DurabilityMonitor::SyncWrite {
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
    SyncWrite(const void* cookie,
              queued_item item,
              std::chrono::milliseconds defaultTimeout,
              const ReplicationChain* firstChain,
              const ReplicationChain* secondChain);

    /**
     * Constructs a SyncWrite with an infinite timeout
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
    explicit SyncWrite(const void* cookie,
                       queued_item item,
                       const ReplicationChain* firstChain,
                       const ReplicationChain* secondChain,
                       InfiniteTimeout);

    const StoredDocKey& getKey() const;

    int64_t getBySeqno() const;

    cb::durability::Requirements getDurabilityReqs() const;

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
    void resetTopology(const ReplicationChain& firstChain,
                       const ReplicationChain* secondChain);

    /**
     * Reset the ack-state for this SyncWrite and set it up for the new given
     * topology if durability is possible for the new chains.
     *
     * @param firstChain Reference to first chain
     * @param secondChain Pointer (may be null) to second chain
     */
    void checkDurabilityPossibleAndResetTopology(
            const ReplicationChain& firstChain,
            const ReplicationChain* secondChain);

    /**
     * Return the names of all nodes which have ack'ed this SyncWrite
     *
     */
    std::unordered_set<std::string> getAckedNodes() const;

    const queued_item& getItem() const {
        return item;
    }

    void setCompleted() {
        completed = true;
    }

    /**
     * @return true if this SyncWrite has been logically completed
     */
    bool isCompleted() const {
        return completed;
    }

private:
    /**
     * Performs sanity checks and initialise the replication chains
     * @param firstChain Pointer (may be null) to the first chain
     * @param secondChain Pointer (may be null) to second chain
     */
    void initialiseChains(const ReplicationChain* firstChain,
                          const ReplicationChain* secondChain);
    /**
     * Calculate the ackCount for this SyncWrite using the given chain.
     *
     * @param chain ReplicationChain to iterate on
     * @return The ackCount that we have for the given chain. Does not count the
     *         active
     */
    uint8_t getAckCountForNewChain(const ReplicationChain& chain);

    // Client cookie associated with this SyncWrite request, to be notified
    // when the SyncWrite completes.
    const void* cookie;

    // An Item stores all the info that the DurabilityMonitor needs:
    // - seqno
    // - Durability Requirements
    // Note that queued_item is a ref-counted object, so the copy in the
    // CheckpointManager can be safely removed.
    const queued_item item;

    /**
     * Holds all the information required for a SyncWrite to determine if it
     * is satisfied by a given chain. We can do all of this using the
     * ReplicationChain, but we store an ackCount as well as an optimization.
     */
    struct ChainStatus {
        operator bool() const {
            return chainPtr;
        }

        void reset(const ReplicationChain* chainPtr, uint8_t ackCount) {
            this->ackCount.reset(ackCount);
            this->chainPtr = chainPtr;
        }

        // Ack counter for the chain.
        // This optimization eliminates the need of scanning the positions map
        // in the ReplicationChain for verifying Durability Requirements.
        Monotonic<uint8_t> ackCount{0};

        // Pointer to the chain. Used to find out which node is the active and
        // what the majority value is.
        const ReplicationChain* chainPtr{nullptr};
    };

    ChainStatus firstChain;
    ChainStatus secondChain;

    // Used for enforcing the Durability Requirements Timeout. It is set
    // when this SyncWrite is added for tracking into the DurabilityMonitor.
    const boost::optional<std::chrono::steady_clock::time_point> expiryTime;

    /// The time point the SyncWrite was added to the DurabilityMonitor.
    /// Used for statistics (track how long SyncWrites take to complete).
    const std::chrono::steady_clock::time_point startTime;

    /**
     * A replica receiving a disk snapshot or a snapshot with a persist level
     * prepare may not remove the SyncWrite object from trackedWrites until
     * it has been persisted. This field exists to distinguish prepares that
     * have been completed but still exist in trackedWrites from those that have
     * not yet been completed.
     */
    bool completed = false;

    friend std::ostream& operator<<(std::ostream&, const SyncWrite&);
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
struct DurabilityMonitor::Position {
    Position() = default;
    Position(const Container::iterator& it) : it(it) {
    }
    Container::iterator it;
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
struct DurabilityMonitor::ReplicationChain {
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
    std::unordered_map<std::string, Position> positions;

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
    State(const PassiveDurabilityMonitor& pdm);

    /**
     * Returns the next position for a given Container::iterator.
     *
     * @param it The iterator
     * @return the next position in Container
     */
    Container::iterator getIteratorNext(const Container::iterator& it);

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

    /// The container of pending Prepares.
    Container trackedWrites;

    // The seqno of the last Prepare satisfied locally. I.e.:
    //     - the Prepare has been queued into the PDM, if Level Majority
    //         or MajorityAndPersistToMaster
    //     - the Prepare has been persisted locally, if Level
    //         PersistToMajority
    Position highPreparedSeqno;

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
    Position highCompletedSeqno;

    const PassiveDurabilityMonitor& pdm;
};
