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

#include "active_durability_monitor.h"
#include "bucket_logger.h"
#include "durability_monitor_impl.h"
#include "item.h"
#include "passive_durability_monitor.h"
#include "stats.h"
#include "statwriter.h"
#include "vbucket.h"

#include <boost/algorithm/string/join.hpp>

#include <gsl.h>

/*
 * This class embeds the state of an ADM. It has been designed for being
 * wrapped by a folly::Synchronized<T>, which manages the read/write
 * concurrent access to the T instance.
 * Note: all members are public as accessed directly only by ADM, this is
 * a protected struct. Avoiding direct access by ADM would require
 * re-implementing most of the ADM functions into ADM::State and exposing
 * them on the ADM::State public interface.
 */
struct ActiveDurabilityMonitor::State {
    /**
     * @param adm The owning ActiveDurabilityMonitor
     */
    State(const ActiveDurabilityMonitor& adm) : adm(adm) {
    }

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

    void setReplicationTopology(const nlohmann::json& topology);

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

    /**
     * Perform the manual ack (from the map of queuedSeqnoAcks) that is
     * required at rebalance for the given chain
     *
     * @param chain Chain for which we should manually ack nodes
     */
    void performQueuedAckForChain(const ReplicationChain& chain);

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
    void advanceAndAckForPosition(Position& pos,
                                  const std::string& node,
                                  bool shouldAck);

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
    Monotonic<int64_t, ThrowExceptionPolicy> lastTrackedSeqno;

    // Stores the last committed seqno.
    Monotonic<int64_t> lastCommittedSeqno = 0;

    // Stores the last aborted seqno.
    Monotonic<int64_t> lastAbortedSeqno = 0;

    // Cumulative count of accepted (tracked) SyncWrites.
    size_t totalAccepted = 0;
    // Cumulative count of Committed SyncWrites.
    size_t totalCommitted = 0;
    // Cumulative count of Aborted SyncWrites.
    size_t totalAborted = 0;

    // The durability timeout value to use for SyncWrites which haven't
    // specified an explicit timeout.
    // @todo-durability: Allow this to be configurable.
    std::chrono::milliseconds defaultTimeout = std::chrono::seconds(30);

    const ActiveDurabilityMonitor& adm;

    // Map of node to seqno value for seqno acks that we have seen but
    // do not exist in the current replication topology. They may be
    // required to manually ack for a new node if we receive an ack before
    // ns_server sends us a new replication topology.
    std::unordered_map<std::string, Monotonic<int64_t>> queuedSeqnoAcks;
};

ActiveDurabilityMonitor::ActiveDurabilityMonitor(EPStats& stats, VBucket& vb)
    : stats(stats), vb(vb), state(std::make_unique<State>(*this)) {
}

ActiveDurabilityMonitor::ActiveDurabilityMonitor(
        EPStats& stats,
        VBucket& vb,
        std::vector<queued_item>&& outstandingPrepares)
    : ActiveDurabilityMonitor(stats, vb) {
    auto s = state.wlock();
    for (auto& prepare : outstandingPrepares) {
        auto seqno = prepare->getBySeqno();
        // Any outstanding prepares "grandfathered" into the DM should have
        // already specified a non-default timeout.
        Expects(!prepare->getDurabilityReqs().getTimeout().isDefault());
        s->trackedWrites.emplace_back(nullptr,
                                      std::move(prepare),
                                      std::chrono::milliseconds{},
                                      nullptr,
                                      nullptr);
        s->lastTrackedSeqno = seqno;
    }
}

ActiveDurabilityMonitor::ActiveDurabilityMonitor(EPStats& stats,
                                                 PassiveDurabilityMonitor&& pdm)
    : ActiveDurabilityMonitor(stats, pdm.vb) {
    auto s = state.wlock();
    s->trackedWrites.swap(pdm.state.wlock()->trackedWrites);
    if (!s->trackedWrites.empty()) {
        s->lastTrackedSeqno = s->trackedWrites.back().getBySeqno();
    }
}

ActiveDurabilityMonitor::~ActiveDurabilityMonitor() = default;

void ActiveDurabilityMonitor::setReplicationTopology(
        const nlohmann::json& topology) {
    Expects(vb.getState() == vbucket_state_active);
    Expects(!topology.is_null());

    if (!topology.is_array()) {
        throw std::invalid_argument(
                "ActiveDurabilityMonitor::setReplicationTopology: Topology is "
                "not an "
                "array");
    }

    if (topology.size() == 0) {
        throw std::invalid_argument(
                "ActiveDurabilityMonitor::setReplicationTopology: Topology is "
                "empty");
    }

    // Setting the replication topology also resets the topology in all
    // in-flight (tracked) SyncWrites. If the new topology contains only the
    // Active, then some Prepares could be immediately satisfied and ready for
    // commit.
    //
    // Note: We must release the lock to state before calling back to VBucket
    // (in commit()) to avoid a lock inversion with HashBucketLock (same issue
    // as at seqnoAckReceived(), details in there).
    //
    // Note: setReplicationTopology + updateHighPreparedSeqno must be a single
    // atomic operation. We could commit out-of-seqno-order Prepares otherwise.
    Container toCommit;
    {
        auto s = state.wlock();
        s->setReplicationTopology(topology);
        toCommit = s->updateHighPreparedSeqno();
    }

    for (const auto& sw : toCommit) {
        commit(sw);
    }
}

int64_t ActiveDurabilityMonitor::getHighPreparedSeqno() const {
    const auto s = state.rlock();
    if (!s->firstChain) {
        return 0;
    }
    return s->getNodeWriteSeqno(s->getActive());
}

int64_t ActiveDurabilityMonitor::getHighCompletedSeqno() const {
    const auto s = state.rlock();
    return std::max(s->lastCommittedSeqno, s->lastAbortedSeqno);
}

bool ActiveDurabilityMonitor::isDurabilityPossible() const {
    const auto s = state.rlock();
    // Durability is only possible if we have a first chain for which
    // durability is possible. If we have a second chain, durability must also
    // be possible for that chain.
    return s->firstChain && s->firstChain->isDurabilityPossible() &&
           (!s->secondChain || s->secondChain->isDurabilityPossible());
}

void ActiveDurabilityMonitor::addSyncWrite(const void* cookie,
                                           queued_item item) {
    auto durReq = item->getDurabilityReqs();

    if (durReq.getLevel() == cb::durability::Level::None) {
        throw std::invalid_argument(
                "ActiveDurabilityMonitor::addSyncWrite: Level::None");
    }

    // The caller must have already checked this and returned a proper error
    // before executing down here. Here we enforce it again for defending from
    // unexpected races between VBucket::setState (which sets the replication
    // topology).
    if (!isDurabilityPossible()) {
        throw std::logic_error(
                "ActiveDurabilityMonitor::addSyncWrite: Impossible");
    }

    state.wlock()->addSyncWrite(cookie, std::move(item));
}

ENGINE_ERROR_CODE ActiveDurabilityMonitor::seqnoAckReceived(
        const std::string& replica, int64_t preparedSeqno) {
    // Note:
    // TSan spotted that in the execution path to DM::addSyncWrites we acquire
    // HashBucketLock first and then a lock to DM::state, while here we
    // acquire first the lock to DM::state and then HashBucketLock.
    // This could cause a deadlock by lock inversion (note that the 2 execution
    // paths are expected to execute in 2 different threads).
    // Given that the HashBucketLock here is acquired in the sub-call to
    // VBucket::commit, then to fix I need to release the lock to DM::state
    // before executing DM::commit.
    //
    // By logic the correct order of processing for every verified SyncWrite
    // would be:
    // 1) check if DurabilityRequirements are satisfied
    // 2) if they are, then commit
    // 3) remove the committed SyncWrite from tracking
    //
    // But, we are in the situation where steps 1 and 3 must execute under lock
    // to m, while step 2 must not.
    //
    // For now as quick fix I solve by inverting the order of steps 2 and 3:
    // 1) check if DurabilityRequirements are satisfied
    // 2) if they are, remove the verified SyncWrite from tracking
    // 3) commit the removed (and verified) SyncWrite
    //
    // I don't manage the scenario where step 3 fails yet (note that DM::commit
    // just throws if an error occurs in the current implementation), so this
    // is a @todo.
    Container toCommit;
    state.wlock()->processSeqnoAck(replica, preparedSeqno, toCommit);

    // Commit the verified SyncWrites
    for (const auto& sw : toCommit) {
        commit(sw);
    }

    return ENGINE_SUCCESS;
}

void ActiveDurabilityMonitor::processTimeout(
        std::chrono::steady_clock::time_point asOf) {
    // @todo: Add support for DurabilityMonitor at Replica
    if (vb.getState() != vbucket_state_active) {
        throw std::logic_error("ActiveDurabilityMonitor::processTimeout: " +
                               vb.getId().to_string() + " state is: " +
                               VBucket::toString(vb.getState()));
    }

    Container toAbort;
    state.wlock()->removeExpired(asOf, toAbort);

    for (const auto& entry : toAbort) {
        abort(entry);
    }
}

void ActiveDurabilityMonitor::notifyLocalPersistence() {
    // We must release the lock to state before calling back to VBucket (in
    // commit()) to avoid a lock inversion with HashBucketLock (same issue as
    // at seqnoAckReceived(), details in there).
    Container toCommit = state.wlock()->updateHighPreparedSeqno();

    for (const auto& sw : toCommit) {
        commit(sw);
    }
}

void ActiveDurabilityMonitor::addStats(const AddStatFn& addStat,
                                       const void* cookie) const {
    char buf[256];

    try {
        const auto vbid = vb.getId().get();

        checked_snprintf(buf, sizeof(buf), "vb_%d:state", vbid);
        add_casted_stat(buf, VBucket::toString(vb.getState()), addStat, cookie);

        const auto s = state.rlock();

        checked_snprintf(buf, sizeof(buf), "vb_%d:num_tracked", vbid);
        add_casted_stat(buf, s->trackedWrites.size(), addStat, cookie);

        checked_snprintf(buf, sizeof(buf), "vb_%d:high_prepared_seqno", vbid);
        add_casted_stat(
                buf, s->getNodeWriteSeqno(s->getActive()), addStat, cookie);

        checked_snprintf(buf, sizeof(buf), "vb_%d:last_tracked_seqno", vbid);
        add_casted_stat(buf, s->lastTrackedSeqno, addStat, cookie);

        checked_snprintf(buf, sizeof(buf), "vb_%d:last_committed_seqno", vbid);
        add_casted_stat(buf, s->lastCommittedSeqno, addStat, cookie);

        checked_snprintf(buf, sizeof(buf), "vb_%d:last_aborted_seqno", vbid);
        add_casted_stat(buf, s->lastAbortedSeqno, addStat, cookie);

        if (s->firstChain) {
            addStatsForChain(addStat, cookie, *s->firstChain.get());
        }
        if (s->secondChain) {
            addStatsForChain(addStat, cookie, *s->secondChain.get());
        }
    } catch (const std::exception& e) {
        EP_LOG_WARN(
                "ActiveDurabilityMonitor::State:::addStats: error building "
                "stats: {}",
                e.what());
    }
}

void ActiveDurabilityMonitor::addStatsForChain(
        const AddStatFn& addStat,
        const void* cookie,
        const ReplicationChain& chain) const {
    char buf[256];
    const auto vbid = vb.getId().get();
    checked_snprintf(buf,
                     sizeof(buf),
                     "vb_%d:replication_chain_%s:size",
                     vbid,
                     to_string(chain.name).c_str());
    add_casted_stat(buf, chain.positions.size(), addStat, cookie);

    for (const auto& entry : chain.positions) {
        const auto* node = entry.first.c_str();
        const auto& pos = entry.second;

        checked_snprintf(buf,
                         sizeof(buf),
                         "vb_%d:replication_chain_%s:%s:last_write_seqno",
                         vbid,
                         to_string(chain.name).c_str(),
                         node);
        add_casted_stat(buf, pos.lastWriteSeqno, addStat, cookie);
        checked_snprintf(buf,
                         sizeof(buf),
                         "vb_%d:replication_chain_%s:%s:last_ack_seqno",
                         vbid,
                         to_string(chain.name).c_str(),
                         node);
        add_casted_stat(buf, pos.lastAckSeqno, addStat, cookie);
    }
}

size_t ActiveDurabilityMonitor::getNumTracked() const {
    return state.rlock()->trackedWrites.size();
}

size_t ActiveDurabilityMonitor::getNumAccepted() const {
    return state.rlock()->totalAccepted;
}
size_t ActiveDurabilityMonitor::getNumCommitted() const {
    return state.rlock()->totalCommitted;
}
size_t ActiveDurabilityMonitor::getNumAborted() const {
    return state.rlock()->totalAborted;
}

uint8_t ActiveDurabilityMonitor::getFirstChainSize() const {
    const auto s = state.rlock();
    return s->firstChain ? s->firstChain->positions.size() : 0;
}

uint8_t ActiveDurabilityMonitor::getSecondChainSize() const {
    const auto s = state.rlock();
    return s->secondChain ? s->secondChain->positions.size() : 0;
}

uint8_t ActiveDurabilityMonitor::getFirstChainMajority() const {
    const auto s = state.rlock();
    return s->firstChain ? s->firstChain->majority : 0;
}

uint8_t ActiveDurabilityMonitor::getSecondChainMajority() const {
    const auto s = state.rlock();
    return s->secondChain ? s->secondChain->majority : 0;
}

void ActiveDurabilityMonitor::removedQueuedAck(const std::string& node) {
    state.wlock()->queuedSeqnoAcks.erase(node);
}

ActiveDurabilityMonitor::Container::iterator
ActiveDurabilityMonitor::State::getNodeNext(const std::string& node) {
    Expects(firstChain.get());
    // Note: Container::end could be the new position when the pointed SyncWrite
    //     is removed from Container and the iterator repositioned.
    //     In that case next=Container::begin
    auto firstChainItr = firstChain->positions.find(node);
    if (firstChainItr != firstChain->positions.end()) {
        const auto& it = firstChainItr->second.it;
        return (it == trackedWrites.end()) ? trackedWrites.begin()
                                           : std::next(it);
    }

    if (secondChain) {
        auto secondChainItr = secondChain->positions.find(node);
        if (secondChainItr != secondChain->positions.end()) {
            const auto& it = secondChainItr->second.it;
            return (it == trackedWrites.end()) ? trackedWrites.begin()
                                               : std::next(it);
        }
    }

    // Node not found, return the trackedWrites.end(), stl style.
    return trackedWrites.end();
}

ActiveDurabilityMonitor::Container::iterator
ActiveDurabilityMonitor::State::advanceNodePosition(const std::string& node) {
    // We must have at least a firstChain
    Expects(firstChain.get());

    // But the node may not be in it if we have a secondChain
    auto firstChainItr = firstChain->positions.find(node);
    auto firstChainFound = firstChainItr != firstChain->positions.end();
    if (!firstChainFound && !secondChain) {
        // Attempting to advance for a node we don't know about, panic
        throw std::logic_error(
                "ActiveDurabilityMonitor::State::advanceNodePosition: "
                "Attempting to advance positions for an invalid node " +
                node);
    }

    std::unordered_map<std::string, Position>::iterator secondChainItr;
    auto secondChainFound = false;
    if (secondChain) {
        secondChainItr = secondChain->positions.find(node);
        secondChainFound = secondChainItr != secondChain->positions.end();
        if (!firstChainFound && !secondChainFound) {
            throw std::logic_error(
                    "ActiveDurabilityMonitor::State::advanceNodePosition "
                    "Attempting to advance positions for an invalid node " +
                    node + ". Node is not in firstChain or secondChain");
        }
    }

    // Node may be in both chains (or only one) so we need to advance only the
    // correct chain.
    if (firstChainFound) {
        auto& pos = const_cast<Position&>(firstChainItr->second);
        // We only ack if we do not have this node in the secondChain because
        // we only want to ack once
        advanceAndAckForPosition(pos, node, !secondChainFound /*should ack*/);
        if (!secondChainFound) {
            return pos.it;
        }
    }

    if (secondChainFound) {
        // Update second chain itr
        auto& pos = const_cast<Position&>(secondChainItr->second);
        advanceAndAckForPosition(pos, node, true /* should ack*/);
        return pos.it;
    }

    folly::assume_unreachable();
}

void ActiveDurabilityMonitor::State::advanceAndAckForPosition(
        Position& pos, const std::string& node, bool shouldAck) {
    if (pos.it == trackedWrites.end()) {
        pos.it = trackedWrites.begin();
    } else {
        pos.it++;
    }

    Expects(pos.it != trackedWrites.end());

    // Note that Position::lastWriteSeqno is always set to the current
    // pointed SyncWrite to keep the replica seqno-state for when the pointed
    // SyncWrite is removed
    pos.lastWriteSeqno = pos.it->getBySeqno();

    // Update the SyncWrite ack-counters, necessary for DurReqs verification
    if (shouldAck) {
        pos.it->ack(node);
    }
}

void ActiveDurabilityMonitor::State::updateNodeAck(const std::string& node,
                                                   int64_t seqno) {
    // We must have at least a firstChain
    Expects(firstChain.get());

    // But the node may not be in it.
    auto firstChainItr = firstChain->positions.find(node);
    auto firstChainFound = firstChainItr != firstChain->positions.end();
    if (firstChainFound) {
        auto& firstChainPos = const_cast<Position&>(firstChainItr->second);
        firstChainPos.lastAckSeqno = seqno;
    }

    bool secondChainFound = false;
    if (secondChain) {
        auto secondChainItr = secondChain->positions.find(node);
        if (secondChainItr != secondChain->positions.end()) {
            secondChainFound = true;
            auto& secondChainPos =
                    const_cast<Position&>(secondChainItr->second);
            secondChainPos.lastAckSeqno = seqno;
        }
    }

    if (!firstChainFound && !secondChainFound) {
        // We didn't find the node in either of our chains, but we still need to
        // track the ack for this node in case we are about to get a topology
        // change in which this node will exist.
        queuedSeqnoAcks[node] = seqno;
    }
}

int64_t ActiveDurabilityMonitor::getNodeWriteSeqno(
        const std::string& node) const {
    return state.rlock()->getNodeWriteSeqno(node);
}

int64_t ActiveDurabilityMonitor::getNodeAckSeqno(
        const std::string& node) const {
    return state.rlock()->getNodeAckSeqno(node);
}

const std::string& ActiveDurabilityMonitor::State::getActive() const {
    Expects(firstChain.get());
    return firstChain->active;
}

int64_t ActiveDurabilityMonitor::State::getNodeWriteSeqno(
        const std::string& node) const {
    Expects(firstChain.get());
    auto firstChainItr = firstChain->positions.find(node);
    if (firstChainItr != firstChain->positions.end()) {
        return firstChainItr->second.lastWriteSeqno;
    }

    if (secondChain) {
        auto secondChainItr = secondChain->positions.find(node);
        if (secondChainItr != secondChain->positions.end()) {
            return secondChainItr->second.lastWriteSeqno;
        }
    }

    throw std::invalid_argument(
            "ActiveDurabilityMonitor::State::getNodeWriteSeqno: "
            "Node " +
            node + " not found");
}

int64_t ActiveDurabilityMonitor::State::getNodeAckSeqno(
        const std::string& node) const {
    Expects(firstChain.get());
    auto firstChainItr = firstChain->positions.find(node);
    if (firstChainItr != firstChain->positions.end()) {
        return firstChainItr->second.lastAckSeqno;
    }

    if (secondChain) {
        auto secondChainItr = secondChain->positions.find(node);
        if (secondChainItr != secondChain->positions.end()) {
            return secondChainItr->second.lastAckSeqno;
        }
    }

    throw std::invalid_argument(
            "ActiveDurabilityMonitor::State::getNodeAckSeqno: "
            "Node " +
            node + " not found");
}

ActiveDurabilityMonitor::Container
ActiveDurabilityMonitor::State::removeSyncWrite(Container::iterator it) {
    if (it == trackedWrites.end()) {
        throw std::logic_error(
                "ActiveDurabilityMonitor::commit: Position points to end");
    }

    Container::iterator prev;
    // Note: iterators in trackedWrites are never singular, Container::end
    //     is used as placeholder element for when an iterator cannot point to
    //     any valid element in Container
    if (it == trackedWrites.begin()) {
        prev = trackedWrites.end();
    } else {
        prev = std::prev(it);
    }

    // Removing the element at 'it' from trackedWrites invalidates any
    // iterator that points to that element. So, we have to reposition the
    // invalidated iterators before proceeding with the removal.
    //
    // Note: O(N) with N=<number of iterators>, max(N)=6
    //     (max 2 chains, 3 replicas, 1 iterator per replica)
    Expects(firstChain.get());
    for (const auto& entry : firstChain->positions) {
        const auto& nodePos = entry.second;
        if (nodePos.it == it) {
            const_cast<Position&>(nodePos).it = prev;
        }
    }

    if (secondChain) {
        for (const auto& entry : secondChain->positions) {
            const auto& nodePos = entry.second;
            if (nodePos.it == it) {
                const_cast<Position&>(nodePos).it = prev;
            }
        }
    }

    Container removed;
    removed.splice(removed.end(), trackedWrites, it);

    return removed;
}

void ActiveDurabilityMonitor::commit(const SyncWrite& sw) {
    const auto& key = sw.getKey();
    auto result = vb.commit(key,
                            {} /*commitSeqno*/,
                            vb.lockCollections(key),
                            sw.getCookie());
    if (result != ENGINE_SUCCESS) {
        throw std::logic_error(
                "ActiveDurabilityMonitor::commit: VBucket::commit failed with "
                "status:" +
                std::to_string(result));
    }

    // Record the duration of the SyncWrite in histogram.
    const auto index = size_t(sw.getDurabilityReqs().getLevel()) - 1;
    const auto commitDuration =
            std::chrono::duration_cast<std::chrono::microseconds>(
                    std::chrono::steady_clock::now() - sw.getStartTime());
    stats.syncWriteCommitTimes.at(index).add(commitDuration);

    {
        auto s = state.wlock();
        s->lastCommittedSeqno = sw.getBySeqno();
        s->totalCommitted++;
        // Note:
        // - Level Majority locally-satisfied first at Active by-logic
        // - Level MajorityAndPersistOnMaster and PersistToMajority must always
        //     include the Active for being globally satisfied
        const auto hps = s->getNodeWriteSeqno(s->getActive());
        Ensures(s->lastCommittedSeqno <= hps);
    }

    if (globalBucketLogger->should_log(spdlog::level::debug)) {
        std::stringstream ss;
        ss << "SyncWrite commit \"" << key << "\": ack'ed by {"
           << boost::join(sw.getAckedNodes(), ", ") << "}";

        EP_LOG_DEBUG(ss.str());
    }
}

void ActiveDurabilityMonitor::abort(const SyncWrite& sw) {
    const auto& key = sw.getKey();
    auto result = vb.abort(key,
                           sw.getBySeqno() /*prepareSeqno*/,
                           {} /*abortSeqno*/,
                           vb.lockCollections(key),
                           sw.getCookie());
    if (result != ENGINE_SUCCESS) {
        throw std::logic_error(
                "ActiveDurabilityMonitor::abort: VBucket::abort failed with "
                "status:" +
                std::to_string(result));
    }
    auto s = state.wlock();
    s->lastAbortedSeqno = sw.getBySeqno();
    s->totalAborted++;
}

std::vector<const void*>
ActiveDurabilityMonitor::getCookiesForInFlightSyncWrites() {
    auto s = state.wlock();
    auto vec = std::vector<const void*>();
    for (auto write : s->trackedWrites) {
        auto* cookie = write.getCookie();
        if (cookie) {
            vec.push_back(cookie);
        }
    }
    return vec;
}

void ActiveDurabilityMonitor::State::processSeqnoAck(const std::string& node,
                                                     int64_t seqno,
                                                     Container& toCommit) {
    if (!firstChain) {
        throw std::logic_error(
                "ActiveDurabilityMonitor::processSeqnoAck: FirstChain not "
                "set");
    }
    if (seqno > lastTrackedSeqno) {
        throw std::invalid_argument(
                "ActiveDurabilityMonitor::processSeqnoAck: seqno(" +
                std::to_string(seqno) + ") is greater than lastTrackedSeqno(" +
                std::to_string(lastTrackedSeqno) + "\"");
    }

    // We should never ack for the active
    Expects(firstChain->active != node);

    // Note: process up to the ack'ed seqno
    ActiveDurabilityMonitor::Container::iterator next;
    while ((next = getNodeNext(node)) != trackedWrites.end() &&
           next->getBySeqno() <= seqno) {
        // Update replica tracking
        const auto& posIt = advanceNodePosition(node);

        // Check if Durability Requirements satisfied now, and add for commit
        if (posIt->isSatisfied()) {
            auto removed = removeSyncWrite(posIt);
            toCommit.splice(toCommit.end(), removed);
        }
    }

    // We keep track of the actual ack'ed seqno
    updateNodeAck(node, seqno);
}

std::unordered_set<int64_t> ActiveDurabilityMonitor::getTrackedSeqnos() const {
    const auto s = state.rlock();
    std::unordered_set<int64_t> ret;
    for (const auto& w : s->trackedWrites) {
        ret.insert(w.getBySeqno());
    }
    return ret;
}

size_t ActiveDurabilityMonitor::wipeTracked() {
    auto s = state.wlock();
    // Note: Cannot just do Container::clear as it would invalidate every
    //     existing Replication Chain iterator
    size_t removed{0};
    Container::iterator it = s->trackedWrites.begin();
    while (it != s->trackedWrites.end()) {
        // Note: 'it' will be invalidated, so it will need to be reset
        const auto next = std::next(it);
        removed += s->removeSyncWrite(it).size();
        it = next;
    }
    return removed;
}

void ActiveDurabilityMonitor::toOStream(std::ostream& os) const {
    const auto s = state.rlock();
    os << "ActiveDurabilityMonitor[" << this
       << "] #trackedWrites:" << s->trackedWrites.size()
       << " lastTrackedSeqno:" << s->lastTrackedSeqno
       << " lastCommittedSeqno:" << s->lastCommittedSeqno
       << " lastAbortedSeqno:" << s->lastAbortedSeqno << "\n";
    for (const auto& w : s->trackedWrites) {
        os << "    " << w << "\n";
    }
    os << "]";
}

void ActiveDurabilityMonitor::validateChain(
        const nlohmann::json& chain,
        DurabilityMonitor::ReplicationChainName chainName) {
    if (chain.size() == 0) {
        throw std::invalid_argument("ActiveDurabilityMonitor::validateChain: " +
                                    to_string(chainName) +
                                    " chain cannot be empty");
    }

    // Max Active + MaxReplica
    if (chain.size() > 1 + maxReplicas) {
        throw std::invalid_argument(
                "ActiveDurabilityMonitor::validateChain: Too many nodes in " +
                to_string(chainName) + " chain: " + chain.dump());
    }

    if (!chain.at(0).is_string()) {
        throw std::invalid_argument(
                "ActiveDurabilityMonitor::validateChain: first node in " +
                to_string(chainName) + " chain (active) cannot be undefined");
    }
}

std::unique_ptr<DurabilityMonitor::ReplicationChain>
ActiveDurabilityMonitor::State::makeChain(
        const DurabilityMonitor::ReplicationChainName name,
        const nlohmann::json& chain) {
    std::vector<std::string> nodes;
    for (auto& node : chain.items()) {
        // First node (active) must be present, remaining (replica) nodes
        // are allowed to be Null indicating they are undefined.
        if (node.value().is_string()) {
            nodes.push_back(node.value());
        } else {
            nodes.emplace_back(UndefinedNode);
        }
    }

    auto ptr = std::make_unique<ReplicationChain>(
            name, nodes, trackedWrites.end());

    // MB-34318
    // The HighPreparedSeqno is the lastWriteSeqno of the active node in the
    // firstChain. This is typically set when we call
    // ADM::State::updateHighPreparedSeqno(). However, it relies on there being
    // trackedWrites to update it. To keep the correct HPS post topology change
    // when there are no trackedWrites (no SyncWrites in flight) we need to
    // manually set the lastWriteSeqno of the active node in the new chain.
    if (name == ReplicationChainName::First) {
        if (!firstChain) {
            return ptr;
        }

        auto firstChainItr = firstChain->positions.find(firstChain->active);
        if (firstChainItr == firstChain->positions.end()) {
            // Sanity - we should never make a chain in this state
            throw std::logic_error(
                    "ADM::State::makeChain did not find the "
                    "active node for the first chain in the "
                    "first chain.");
        }

        auto newChainItr = ptr->positions.find(ptr->active);
        if (newChainItr == ptr->positions.end()) {
            // Sanity - we should never make a chain in this state
            throw std::logic_error(
                    "ADM::State::makeChain did not find the "
                    "active node for the first chain in the "
                    "new chain.");
        }

        // We set the lastWriteSeqno (HPS) on the new chain regardless of
        // whether not the firstChain active has changed. If it does, this is
        // ns_server renaming us. Any other change would involve a change of
        // the vBucket state.
        newChainItr->second.lastWriteSeqno =
                firstChainItr->second.lastWriteSeqno;
    }

    return ptr;
}

void ActiveDurabilityMonitor::State::setReplicationTopology(
        const nlohmann::json& topology) {
    auto& fChain = topology.at(0);
    ActiveDurabilityMonitor::validateChain(
            fChain, DurabilityMonitor::ReplicationChainName::First);

    // We need to temporarily hold on to the previous chain so that we can
    // calculate the new ackCount for each SyncWrite. Create the new chain in a
    // temporary variable to do this.
    std::unique_ptr<ReplicationChain> newSecondChain;

    // Check if we should have a second replication chain.
    if (topology.size() > 1) {
        if (topology.size() > 2) {
            // Too many chains specified
            throw std::invalid_argument(
                    "ActiveDurabilityMonitor::State::setReplicationTopology: "
                    "Too many chains specified");
        }

        auto& sChain = topology.at(1);
        ActiveDurabilityMonitor::validateChain(
                sChain, DurabilityMonitor::ReplicationChainName::Second);
        newSecondChain = makeChain(
                DurabilityMonitor::ReplicationChainName::Second, sChain);
    }

    // Only set the firstChain after validating (and setting) the second so that
    // we throw and abort a state change before setting anything. We need to
    // temporarily hold on to the previous chain so that we can calculate the
    // new ackCount for each SyncWrite. Create the new chain in a
    // temporary variable to do this.
    auto newFirstChain =
            makeChain(DurabilityMonitor::ReplicationChainName::First, fChain);

    // @TODO we must check before calling write.resetTopology if durability is
    // possible for the new topology. If it is now, we should abort the in
    // flight sync writes.

    // Apply the new topology to all in-flight SyncWrites
    for (auto& write : trackedWrites) {
        write.resetTopology(*newFirstChain, newSecondChain.get());
    }

    // We have now reset all the topology for SyncWrites so we can dispose of
    // the old chain (by overwriting it with the new one).
    firstChain = std::move(newFirstChain);
    secondChain = std::move(newSecondChain);

    // Manually ack any nodes that did not previously exist in either chain
    performQueuedAckForChain(*firstChain);

    if (secondChain) {
        performQueuedAckForChain(*secondChain);
    }
}

void ActiveDurabilityMonitor::State::performQueuedAckForChain(
        const DurabilityMonitor::ReplicationChain& chain) {
    for (const auto& node : chain.positions) {
        auto existingAck = queuedSeqnoAcks.find(node.first);
        if (existingAck != queuedSeqnoAcks.end()) {
            Container toCommit;
            processSeqnoAck(existingAck->first, existingAck->second, toCommit);
            // ======================= FIRST CHAIN =============================
            // @TODO MB-34318 this should no longer be true and we will need
            // to remove the pre-condition check.
            //
            // This is a little bit counter-intuitive. We may actually need to
            // commit something post-topology change, however, because we have
            // reset the ackCount of all in flight SyncWrites previously we
            // should never ack here. If we had Replicas=1 then we would have
            // already committed due to active ack or would require an active
            // ack (PERSIST levels) to commit. So, if we do commit something as
            // a result of a topology change it will only be done when we move
            // the HighPreparedSeqno. The active can never exist in the
            // queuedSeqnoAcks map so we should also never attempt to ack it
            // here.
            // ===================== SECOND CHAIN ==============================
            // We don't expect any SyncWrite to currently need committing. Why?
            // We require that a SyncWrite must satisfy both firstChain and
            // secondChain. The SyncWrite should have already been committed
            // if the firstChain is satisfied and we are under a vbState lock
            // which will block seqno acks until this topology change has been
            // completed.
            Expects(toCommit.empty());

            // Remove the existingAck, we don't need to track it any further as
            // it is in a chain.
            queuedSeqnoAcks.erase(existingAck);
        }
    }
}

void ActiveDurabilityMonitor::State::addSyncWrite(const void* cookie,
                                                  queued_item item) {
    Expects(firstChain.get());
    const auto seqno = item->getBySeqno();
    trackedWrites.emplace_back(cookie,
                               std::move(item),
                               defaultTimeout,
                               firstChain.get(),
                               secondChain.get());
    lastTrackedSeqno = seqno;
    totalAccepted++;
}

void ActiveDurabilityMonitor::State::removeExpired(
        std::chrono::steady_clock::time_point asOf, Container& expired) {
    Container::iterator it = trackedWrites.begin();
    while (it != trackedWrites.end()) {
        if (it->isExpired(asOf)) {
            // Note: 'it' will be invalidated, so it will need to be reset
            const auto next = std::next(it);

            auto removed = removeSyncWrite(it);
            expired.splice(expired.end(), removed);

            it = next;
        } else {
            ++it;
        }
    }
}

DurabilityMonitor::Container
ActiveDurabilityMonitor::State::updateHighPreparedSeqno() {
    // Note: All the logic below relies on the fact that HPS for Active is
    //     implicitly the tracked position for Active in FirstChain

    if (trackedWrites.empty()) {
        return {};
    }

    const auto& active = getActive();
    DurabilityMonitor::Container toCommit;
    // Check if Durability Requirements are satisfied for the Prepare currently
    // tracked for Active, and add for commit in case.
    auto removeForCommitIfSatisfied =
            [this, &active, &toCommit]() mutable -> void {
        Expects(firstChain.get());
        const auto& pos = firstChain->positions.at(active);
        Expects(pos.it != trackedWrites.end());
        if (pos.it->isSatisfied()) {
            auto removed = removeSyncWrite(pos.it);
            toCommit.splice(toCommit.end(), removed);
        }
    };

    Container::iterator next;
    // First, blindly move HPS up to high-persisted-seqno. Note that here we
    // don't need to check any Durability Level: persistence makes
    // locally-satisfied all the pending Prepares up to high-persisted-seqno.
    while ((next = getNodeNext(active)) != trackedWrites.end() &&
           static_cast<uint64_t>(next->getBySeqno()) <=
                   adm.vb.getPersistenceSeqno()) {
        advanceNodePosition(active);
        removeForCommitIfSatisfied();
    }

    // Then, move the HPS to the last Prepare with Level == Majority.
    // I.e., all the Majority Prepares that were blocked by non-satisfied
    // PersistToMajority and MajorityAndPersistToMaster Prepares are implicitly
    // satisfied now. The first non-satisfied Prepare is the first
    // PersistToMajority or MajorityAndPersistToMaster not covered by
    // persisted-seqno.
    while ((next = getNodeNext(active)) != trackedWrites.end()) {
        const auto level = next->getDurabilityReqs().getLevel();
        Expects(level != cb::durability::Level::None);

        // Note: We are in the ActiveDM. The first Level::PersistToMajority
        // or Level::MajorityAndPersistOnMaster write is our durability-fence.
        if (level == cb::durability::Level::PersistToMajority ||
            level == cb::durability::Level::MajorityAndPersistOnMaster) {
            break;
        }

        advanceNodePosition(active);
        removeForCommitIfSatisfied();
    }

    // Note: For Consistency with the HPS at Replica, I don't update the
    //     Position::lastAckSeqno for the local (Active) tracking.

    return toCommit;
}

void ActiveDurabilityMonitor::checkForCommit() {
    Container toCommit = state.wlock()->updateHighPreparedSeqno();

    // @todo: Consider to commit in a dedicated function for minimizing
    //     contention on front-end threads, as this function is supposed to
    //     execute under VBucket-level lock.
    for (const auto& sw : toCommit) {
        commit(sw);
    }
}
