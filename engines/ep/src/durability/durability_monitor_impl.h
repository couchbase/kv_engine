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

#include <chrono>
#include <unordered_map>

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
     * @param (optional) firstChain The repl-chain that the write is tracked
     *     against.  Necessary at Active for verifying the SW Durability
     *     Requirements.
     * @param (optional) secondChain The second repl-chain that the write is
     *     tracked against. Necessary at Active for verifying the SW Durability
     *     Requirements.
     */
    SyncWrite(const void* cookie,
              queued_item item,
              const ReplicationChain* firstChain,
              const ReplicationChain* secondChain);

    const StoredDocKey& getKey() const;

    int64_t getBySeqno() const;

    cb::durability::Requirements getDurabilityReqs() const;

    const void* getCookie() const;

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
     * given topology.
     *
     * @param firstChain
     * @param secondChain
     */
    void resetTopology(const ReplicationChain& firstChain,
                       const ReplicationChain* secondChain);

private:
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

        void reset(const ReplicationChain* chainPtr) {
            ackCount.reset(0);
            this->chainPtr = chainPtr;
        }

        // Ack counter for the chain.
        // This optimization eliminates the need of scanning the positions map
        // in the ReplicationChain for verifying Durability Requirements.
        Monotonic<uint8_t> ackCount{0};

        // Pointer to the chain. Used to find out which node is the active and
        // what the majority value is.
        const ReplicationChain* chainPtr;
    };

    ChainStatus firstChain;
    ChainStatus secondChain;

    // Used for enforcing the Durability Requirements Timeout. It is set
    // when this SyncWrite is added for tracking into the DurabilityMonitor.
    const boost::optional<std::chrono::steady_clock::time_point> expiryTime;

    friend std::ostream& operator<<(std::ostream&, const SyncWrite&);
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
     */
    ReplicationChain(const DurabilityMonitor::ReplicationChainName name,
                     const std::vector<std::string>& nodes,
                     const Container::iterator& initPos);

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

    // Name of the chain
    const DurabilityMonitor::ReplicationChainName name;
};

std::string to_string(DurabilityMonitor::ReplicationChainName name);
