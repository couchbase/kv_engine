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
 */
#pragma once

#include "durability_monitor.h"
#include "durability_monitor_impl.h"

#include "memcached/engine_error.h"

#include <folly/Synchronized.h>
#include <nlohmann/json.hpp>

#include <unordered_set>

class PassiveDurabilityMonitor;
class VBucket;

/*
 * The DurabilityMonitor for Active VBuckets.
 *
 * The ActiveDurabilityMonitor (ADM) drives the completion (commit/abort) of
 * SyncWrites requests. To do that, the ADM tracks the pending SyncWrites queued
 * at Active and the ACKs sent by Replicas to verify if the Durability
 * Requirements are satisfied for the tracked mutations.
 */
class ActiveDurabilityMonitor : public DurabilityMonitor {
public:
    // Note: constructor and destructor implementation in the .cc file to allow
    // the forward declaration of ReplicationChain in the header
    ActiveDurabilityMonitor(VBucket& vb);

    /**
     * Construct an ActiveDM by converting the given PassiveDM.
     * All the (in-flight) tracked Prepares in the old PassiveDM are retained.
     *
     * @param pdm The PassiveDM to be converted
     */
    ActiveDurabilityMonitor(PassiveDurabilityMonitor&& pdm);

    ~ActiveDurabilityMonitor();

    /**
     * Sets the Replication Topology.
     *
     * @param topology The topology encoded as nlohmann::json array of (max 2)
     *     replication chains. Each replication chain is itself a
     *     nlohmann::json array of nodes representing the chain.
     * @throw std::invalid_argument
     */
    void setReplicationTopology(const nlohmann::json& topology);

    /// @returns the high_prepared_seqno.
    int64_t getHighPreparedSeqno() const override;

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
    void addSyncWrite(const void* cookie, queued_item item);

    /**
     * Expected to be called by memcached at receiving a DCP_SEQNO_ACK packet.
     *
     * @param replica The replica that sent the ACK
     * @param diskSeqno The ack'ed prepared seqno.
     * @return ENGINE_SUCCESS if the operation succeeds, an error code otherwise
     * @throw std::logic_error if the received seqno is unexpected
     */
    ENGINE_ERROR_CODE seqnoAckReceived(const std::string& replica,
                                       int64_t preparedSeqno);

    /**
     * Enforce timeout for the expired SyncWrites in the tracked list.
     *
     * @param asOf The time to be compared with tracked-SWs' expiry-time
     * @throw std::logic_error
     */
    void processTimeout(std::chrono::steady_clock::time_point asOf);

    void notifyLocalPersistence() override;

    /**
     * Output DurabiltyMonitor stats.
     *
     * @param addStat the callback to memcached
     * @param cookie
     */
    void addStats(const AddStatFn& addStat, const void* cookie) const override;

    size_t getNumTracked() const override;

    /**
     * @return the size of FirstChain
     */
    uint8_t getFirstChainSize() const;

    /**
     * @return the FirstChain Majority
     */
    uint8_t getFirstChainMajority() const;

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

protected:
    void toOStream(std::ostream& os) const override;

    /**
     * Commit the given SyncWrite.
     *
     * @param sw The SyncWrite to commit
     */
    void commit(const SyncWrite& sw);

    /**
     * Abort the given SyncWrite.
     *
     * @param sw The SyncWrite to abort
     */
    void abort(const SyncWrite& sw);

    /**
     * Test only (for now; shortly this will be probably needed at rollback).
     * Removes all SyncWrites from the tracked container. Replication chain
     * iterators stay valid.
     *
     * @returns the number of SyncWrites removed from tracking
     */
    size_t wipeTracked();

    /*
     * This class embeds the state of an ADM. It has been designed for being
     * wrapped by a folly::Synchronized<T>, which manages the read/write
     * concurrent access to the T instance.
     * Note: all members are public as accessed directly only by ADM, this is
     * a protected struct. Avoiding direct access by ADM would require
     * re-implementing most of the ADM functions into ADM::State and exposing
     * them on the ADM::State public interface.
     */
    struct State {
        /**
         * @param adm The owning ActiveDurabilityMonitor
         */
        State(const ActiveDurabilityMonitor& adm) : adm(adm) {
        }

        void setReplicationTopology(const nlohmann::json& topology);

        void addSyncWrite(const void* cookie, queued_item item);

        /**
         * Returns the next position for a node iterator.
         *
         * @param node
         * @return the iterator to the next position for the given node
         */
        Container::iterator getNodeNext(const std::string& node);

        /**
         * Advance a node tracking to the next Position in the tracked
         * Container. Note that a Position tracks a node in terms of both:
         * - iterator to a SyncWrite in the tracked Container
         * - seqno of the last SyncWrite ack'ed by the node
         * This function advances both iterator and seqno.
         *
         * @param node
         */
        void advanceNodePosition(const std::string& node);

        /**
         * This function updates the tracking with the last seqno ack'ed by
         * node.
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
         * @param [out] toCommit
         */
        void processSeqnoAck(const std::string& node,
                             int64_t ackSeqno,
                             Container& toCommit);

        /**
         * Removes all the expired Prepares from tracking.
         *
         * @param asOf The time to be compared with tracked-SWs' expiry-time
         * @param [out] the list of the expired Prepares
         */
        void removeExpired(std::chrono::steady_clock::time_point asOf,
                           Container& expired);

        const std::string& getActive() const;

        int64_t getNodeWriteSeqno(const std::string& node) const;

        int64_t getNodeAckSeqno(const std::string& node) const;

        /**
         * Remove the given SyncWrte from tracking.
         *
         * @param it The iterator to the SyncWrite to be removed
         * @return single-element list of the removed SyncWrite.
         */
        Container removeSyncWrite(Container::iterator it);

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
         * @return the Prepares satisfied (ready for commit) by the HPS update
         */
        Container updateHighPreparedSeqno();

        /// The container of pending Prepares.
        Container trackedWrites;
        // @todo: Expand for supporting the SecondChain.
        std::unique_ptr<ReplicationChain> firstChain;
        // Always stores the seqno of the last SyncWrite added for tracking.
        // Useful for sanity checks, necessary because the tracked container
        // can by emptied by Commit/Abort.
        Monotonic<int64_t, ThrowExceptionPolicy> lastTrackedSeqno;

        const ActiveDurabilityMonitor& adm;
    };

    // The VBucket owning this DurabilityMonitor instance
    VBucket& vb;

    folly::Synchronized<State> state;

    const size_t maxReplicas = 3;

    // @todo: Try to remove this, currenlty necessary for testing wipeTracked()
    friend class ActiveDurabilityMonitorTest;
};
