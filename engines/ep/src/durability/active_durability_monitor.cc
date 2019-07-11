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
#include <folly/concurrency/UnboundedQueue.h>

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
        const auto prefix =
                "ActiveDM(" + adm.vb.getId().to_string() + ")::State::";
        lastCommittedSeqno.setLabel(prefix + "lastCommittedSeqno");
        lastAbortedSeqno.setLabel(prefix + "lastAbortedSeqno");
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

    /**
     * Set the replication topology from the given json. If the new topology
     * makes durability impossible then this function will abort any in-flight
     * SyncWrites by enqueuing them in the CompletedQueue toAbort.
     *
     * @param topology Json topology
     * @param toAbort Reference to the completedQueue so that we can abort any
     *                SyncWrites for which durability is no longer possible.
     */
    void setReplicationTopology(const nlohmann::json& topology,
                                CompletedQueue& toAbort);

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
                         CompletedQueue& toCommit);

    /**
     * Removes expired Prepares from tracking which are eligable to be timed
     * out (and Aborted).
     *
     * @param asOf The time to be compared with tracked-SWs' expiry-time
     * @param [out] the CompletedQueue to enqueue the expired Prepares onto.
     */
    void removeExpired(std::chrono::steady_clock::time_point asOf,
                       CompletedQueue& expired);

    /// @returns the name of the active node. Assumes the first chain is valid.
    const std::string& getActive() const;

    int64_t getNodeWriteSeqno(const std::string& node) const;

    int64_t getNodeAckSeqno(const std::string& node) const;

    /**
     * Remove the given SyncWrte from tracking.
     *
     * @param it The iterator to the SyncWrite to be removed
     * @return the removed SyncWrite.
     */
    SyncWrite removeSyncWrite(Container::iterator it);

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
     * @param completed The CompletedQueue to enqueue all prepares satisfied
     *        (ready for commit) by the HPS update
     */
    void updateHighPreparedSeqno(CompletedQueue& completed);

    /**
     * Perform the manual ack (from the map of queuedSeqnoAcks) that is
     * required at rebalance for the given chain
     *
     * @param chain Chain for which we should manually ack nodes
     */
    void performQueuedAckForChain(const ReplicationChain& chain);

    void updateHighCompletedSeqno();

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
    Monotonic<int64_t, ThrowExceptionPolicy> lastTrackedSeqno = 0;

    // Stores the last committed seqno.
    Monotonic<int64_t> lastCommittedSeqno = 0;

    // Stores the last aborted seqno.
    Monotonic<int64_t> lastAbortedSeqno = 0;

    // Stores the highPreparedSeqno
    WeaklyMonotonic<int64_t> highPreparedSeqno = 0;

    // Stores the highCompletedSeqno
    Monotonic<int64_t> highCompletedSeqno = 0;

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

/**
 * Single-Producer / Single-Consumer Queue of completed SyncWrites.
 *
 * When a SyncWrite has been completed (Committed / Aborted) it is and moved
 * from ActiveDM::State::trackedWrites to this class (enqueued).
 *
 * SyncWrites must be completed (produced) in the same order they were tracked,
 * hence there is a single producer, which is enforced by needing to acquire the
 * State::lock when moving items from trackedWrites to the CompletedQueue;
 * and by recording the highEnqueuedSeqno which must never decrement.
 *
 * SyncWrites must also be committed/aborted (consumed) in-order, as we must
 * enqueue them into the CheckpointManager (where seqnos are assigned) in the
 * same order they were removed from the trackedWrites . This is enforced by
 * a 'consumer' mutex which must be acquired to consume items.
 */
class ActiveDurabilityMonitor::CompletedQueue {
public:
    /// Lock which must be acquired to consume (dequeue) items from the queue.
    using ConsumerLock = std::mutex;

    CompletedQueue(Vbid vbid) {
        highEnqueuedSeqno.setLabel("ActiveDM::CompletedQueue[" +
                                   vbid.to_string() + "]");
    }

    /**
     * Enqueue a (completed) SyncWrite onto the queue.
     *
     * @param state ActiveDM state from which the SyncWrite is being moved from.
     *        Required to enforce a single producer; by virtue of having the
     *        State locked.
     * @param sw SyncWrite which has been completed.
     */
    void enqueue(const ActiveDurabilityMonitor::State& state, SyncWrite&& sw) {
        highEnqueuedSeqno = sw.getBySeqno();
        queue.enqueue(sw);
    }

    /**
     * Attempt to dequeue (consume) a SyncWrite from the queue. Returns a valid
     * optional if there is an item available to dequeue, otherwise returns
     * an empty optional.
     *
     * @param clg Consumer lock guard which must be acquired to attempt
     *            consumuption (to enforce single consumer).
     * @return The oldest item on the queue if the queue is non-empty, else
     *         an empty optional.
     */
    folly::Optional<SyncWrite> try_dequeue(
            const std::lock_guard<ConsumerLock>& clg) {
        return queue.try_dequeue();
    }

    /// @returns a reference to the consumer lock (required to dequeue items).
    ConsumerLock& getConsumerLock() {
        return consumerLock;
    }

    /// @returns true if the queue is currently empty.
    bool empty() const {
        return queue.empty();
    }

private:
    // Unbounded, Single-producer, single-consumer Queue of SyncWrite objects,
    // non-blocking variant.
    using Queue = folly::USPSCQueue<DurabilityMonitor::SyncWrite, false>;
    Queue queue;
    // Track the highest Enqueued Seqno to enforce enqueue ordering.
    Monotonic<int64_t> highEnqueuedSeqno = {0};

    /// The lock guarding consumption of items.
    ConsumerLock consumerLock;
};

ActiveDurabilityMonitor::ActiveDurabilityMonitor(EPStats& stats, VBucket& vb)
    : stats(stats),
      vb(vb),
      state(std::make_unique<State>(*this)),
      completedQueue(std::make_unique<CompletedQueue>(vb.getId())) {
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
    } else {
        // If we have no tracked writes then the last tracked should be the last
        // completed. Reset in case we had no SyncWrites (0 -> 0).
        s->lastTrackedSeqno.reset(
                pdm.state.wlock()->highCompletedSeqno.lastWriteSeqno);
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
    // Note: We must release the lock to state before calling back to
    // VBucket::commit() (via processCompletedSyncWriteQueue) to avoid a lock
    // inversion with HashBucketLock (same issue as at seqnoAckReceived(),
    // details in there).
    //
    // Note: setReplicationTopology + updateHighPreparedSeqno must be a single
    // atomic operation. We could commit out-of-seqno-order Prepares otherwise.
    {
        auto s = state.wlock();
        s->setReplicationTopology(topology, *completedQueue);
        s->updateHighPreparedSeqno(*completedQueue);
    }

    processCompletedSyncWriteQueue();
}

int64_t ActiveDurabilityMonitor::getHighPreparedSeqno() const {
    const auto s = state.rlock();
    if (!s->firstChain) {
        return 0;
    }
    return s->highPreparedSeqno;
}

int64_t ActiveDurabilityMonitor::getHighCompletedSeqno() const {
    return state.rlock()->highCompletedSeqno;
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
    // By logic the correct order of processing for every verified SyncWrite
    // would be:
    // 1) check if DurabilityRequirements are satisfied
    // 2) if they are, then commit
    // 3) remove the committed SyncWrite from tracking
    //
    // But, we are in the situation where steps 1 and 3 must execute under the
    // State lock, while step 2 must not to avoid lock-order inversion:
    // Step 2 requires we acquire the appropriate HashBucketLock inside
    // VBucket::commit(), however in ActiveDM::addSyncWrite() it is called
    // with HashBucketLock already acquired and *then* we acquire State lock.
    // As such we cannot acquire the locks in the opposite order here.
    //
    // To address this, we implement the above sequence as:
    // 1) and 3) Move satisfied SyncWrites from State::trackedWrites to
    //           completedQueue (while State and completedQueue are both
    //           locked).
    // 2) Lock completedQueue, then commit each item and remove from queue.
    //
    // This breaks the potential lock order inversion cycle, as we never acquire
    // both HashBucketLock and State lock together in this function.
    //
    // I don't manage the scenario where step 3 fails yet (note that DM::commit
    // just throws if an error occurs in the current implementation), so this
    // is a @todo.

    // Identify all SyncWrites which are committed by this seqnoAck,
    // transferring them into the completedQueue (under the correct locks).
    state.wlock()->processSeqnoAck(replica, preparedSeqno, *completedQueue);

    if (seqnoAckReceivedPostProcessHook) {
        seqnoAckReceivedPostProcessHook();
    }

    // Process the Completed Queue, committing all items and removing them.
    processCompletedSyncWriteQueue();

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

    // Identify SyncWrites which can be timed out as of this time point
    // and should be aborted, transferring them into the completedQeuue (under
    // the correct locks).
    state.wlock()->removeExpired(asOf, *completedQueue);

    processCompletedSyncWriteQueue();
}

void ActiveDurabilityMonitor::notifyLocalPersistence() {
    checkForCommit();
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

        // Do not have a valid HPS unless the first chain has been set.
        int64_t highPreparedSeqno = 0;
        if (s->firstChain) {
            highPreparedSeqno = s->getNodeWriteSeqno(s->getActive());
        }
        add_casted_stat(buf, highPreparedSeqno, addStat, cookie);

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

void ActiveDurabilityMonitor::processCompletedSyncWriteQueue() {
    std::lock_guard<CompletedQueue::ConsumerLock> lock(
            completedQueue->getConsumerLock());
    while (folly::Optional<SyncWrite> sw = completedQueue->try_dequeue(lock)) {
        if (sw->isSatisfied()) {
            commit(*sw);
        } else {
            abort(*sw);
        }
    };
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

    // Add a trace event for the ACK from this node (assuming we have a cookie
    // // for it).
    // ActiveDM has no visibility of when a replica was sent the prepare
    // (that's managed by CheckpointManager which doesn't know the client
    // cookie) so just make the start+end the same.
    auto* cookie = pos.it->getCookie();
    if (cookie) {
        const auto ackTime = std::chrono::steady_clock::now();
        const auto event =
                (node == getActive())
                        ? cb::tracing::TraceCode::SYNC_WRITE_ACK_LOCAL
                        : cb::tracing::TraceCode::SYNC_WRITE_ACK_REMOTE;
        TracerStopwatch ackTimer(cookie, event);
        ackTimer.start(ackTime);
        ackTimer.stop(ackTime);
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

DurabilityMonitor::SyncWrite ActiveDurabilityMonitor::State::removeSyncWrite(
        Container::iterator it) {
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
    return std::move(removed.front());
}

void ActiveDurabilityMonitor::commit(const SyncWrite& sw) {
    const auto& key = sw.getKey();

    const auto prepareEnd = std::chrono::steady_clock::now();
    auto* cookie = sw.getCookie();
    if (cookie) {
        // Record a Span for the prepare phase duration. We do this before
        // actually calling VBucket::commit() as we want to add a TraceSpan to
        // the cookie before the response to the client is actually sent (and we
        // report the end of the request), which is done within
        // VBucket::commit().
        TracerStopwatch prepareDuration(
                cookie, cb::tracing::TraceCode::SYNC_WRITE_PREPARE);
        prepareDuration.start(sw.getStartTime());
        prepareDuration.stop(prepareEnd);
    }
    auto result = vb.commit(key,
                            sw.getBySeqno() /*prepareSeqno*/,
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
                    prepareEnd - sw.getStartTime());
    stats.syncWriteCommitTimes.at(index).add(commitDuration);

    {
        auto s = state.wlock();
        s->lastCommittedSeqno = sw.getBySeqno();
        s->updateHighCompletedSeqno();
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
    s->updateHighCompletedSeqno();
    s->totalAborted++;
}

std::vector<const void*>
ActiveDurabilityMonitor::getCookiesForInFlightSyncWrites() {
    auto s = state.wlock();
    auto vec = std::vector<const void*>();
    for (auto& write : s->trackedWrites) {
        auto* cookie = write.getCookie();
        if (cookie) {
            vec.push_back(cookie);
            write.clearCookie();
        }
    }
    return vec;
}

void ActiveDurabilityMonitor::State::processSeqnoAck(const std::string& node,
                                                     int64_t seqno,
                                                     CompletedQueue& toCommit) {
    if (!firstChain) {
        throw std::logic_error(
                "ActiveDurabilityMonitor::processSeqnoAck: FirstChain not "
                "set");
    }
    if (seqno > lastTrackedSeqno) {
        throw std::invalid_argument(
                "ActiveDurabilityMonitor::processSeqnoAck: seqno(" +
                std::to_string(seqno) + ") is greater than lastTrackedSeqno(" +
                std::to_string(lastTrackedSeqno) + ")");
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
            toCommit.enqueue(*this, removeSyncWrite(posIt));
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
        s->removeSyncWrite(it);
        removed++;
        it = next;
    }
    return removed;
}

void ActiveDurabilityMonitor::toOStream(std::ostream& os) const {
    const auto s = state.rlock();
    os << "ActiveDurabilityMonitor[" << this
       << "] #trackedWrites:" << s->trackedWrites.size()
       << " highPreparedSeqno:" << s->highPreparedSeqno
       << " highCompletedSeqno:" << s->highCompletedSeqno
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
        const nlohmann::json& topology, CompletedQueue& toAbort) {
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

    // Apply the new topology to all in-flight SyncWrites
    for (auto& write : trackedWrites) {
        write.resetTopology(*newFirstChain, newSecondChain.get());
    }

    // If durability is not possible for the new chains, then we should abort
    // any in-flight SyncWrites that do not have an infinite timeout so that the
    // client can decide what to do. We do not abort and infinite timeout
    // SyncWrites as we MUST complete them as they exist due to a warmup or
    // Passive->Active transition. We have already reset the topology of the in
    // flight SyncWrites so that they do not contain any invalid pointers post
    // topology change.
    if (!(newFirstChain && newFirstChain->isDurabilityPossible() &&
          (!newSecondChain || newSecondChain->isDurabilityPossible()))) {
        // We can't use a for loop with iterators here because they will be
        // modified to point to invalid memory as we use std::list.splice in
        // removeSyncWrite.
        auto itr = trackedWrites.begin();
        while (itr != trackedWrites.end()) {
            if (!itr->getDurabilityReqs().getTimeout().isInfinite()) {
                // Grab the next itr before we overwrite ours to point to a
                // different list.
                auto next = std::next(itr);
                toAbort.enqueue(*this, removeSyncWrite(trackedWrites.begin()));
                itr = next;
            } else {
                itr++;
            }
        }
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
            CompletedQueue toCommit(adm.vb.getId());
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
        std::chrono::steady_clock::time_point asOf, CompletedQueue& expired) {
    // Given SyncWrites must complete In-Order, iterate from the beginning
    // of trackedWrites only as long as we find expired items; if we encounter
    // any unexpired items then must stop.
    Container::iterator it = trackedWrites.begin();
    while (it != trackedWrites.end()) {
        if (it->isExpired(asOf)) {
            // Note: 'it' will be invalidated, so it will need to be reset
            const auto next = std::next(it);

            expired.enqueue(*this, removeSyncWrite(it));

            it = next;
        } else {
            // Encountered an unexpired item - must stop.
            break;
        }
    }
}

void ActiveDurabilityMonitor::State::updateHighPreparedSeqno(
        CompletedQueue& completed) {
    // Note: All the logic below relies on the fact that HPS for Active is
    //     implicitly the tracked position for Active in FirstChain

    if (trackedWrites.empty()) {
        return;
    }

    if (!firstChain) {
        // An ActiveDM _may_ legitimately have no topology information, if
        // for example it has just been created from a PassiveDM during takeover
        // and ns_server has not yet updated the VBucket's topology.
        // In this case it's not yet possible update HPS; so simply skip.
        // Note: when topology *is* set via setReplicationTopology() then this
        // function is called again to update HPS as appropriate.
        return;
    }

    const auto& active = getActive();
    // Check if Durability Requirements are satisfied for the Prepare currently
    // tracked for Active, and add for commit in case.
    auto removeForCommitIfSatisfied =
            [this, &active, &completed]() mutable -> void {
        Expects(firstChain.get());
        const auto& pos = firstChain->positions.at(active);
        Expects(pos.it != trackedWrites.end());
        if (pos.it->isSatisfied()) {
            completed.enqueue(*this, removeSyncWrite(pos.it));
        }
    };

    Container::iterator next;
    // First, blindly move HPS up to high-persisted-seqno. Note that here we
    // don't need to check any Durability Level: persistence makes
    // locally-satisfied all the pending Prepares up to high-persisted-seqno.
    while ((next = getNodeNext(active)) != trackedWrites.end() &&
           static_cast<uint64_t>(next->getBySeqno()) <=
                   adm.vb.getPersistenceSeqno()) {
        highPreparedSeqno = next->getBySeqno();
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

        highPreparedSeqno = next->getBySeqno();
        advanceNodePosition(active);
        removeForCommitIfSatisfied();
    }

    // Note: For Consistency with the HPS at Replica, I don't update the
    //     Position::lastAckSeqno for the local (Active) tracking.
}

void ActiveDurabilityMonitor::State::updateHighCompletedSeqno() {
    highCompletedSeqno = std::max(lastCommittedSeqno, lastAbortedSeqno);
}

void ActiveDurabilityMonitor::checkForCommit() {
    // Identify all SyncWrites which are now committed, transferring them into
    // the completedQueue (under the correct locks).
    state.wlock()->updateHighPreparedSeqno(*completedQueue);

    // @todo: Consider to commit in a dedicated function for minimizing
    //     contention on front-end threads, as this function is supposed to
    //     execute under VBucket-level lock.

    processCompletedSyncWriteQueue();
}
