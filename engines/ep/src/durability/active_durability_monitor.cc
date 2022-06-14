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

#include "active_durability_monitor.h"
#include "bucket_logger.h"
#include "checkpoint_manager.h"
#include "collections/vbucket_manifest_handles.h"
#include "durability_monitor_impl.h"
#include "item.h"
#include "passive_durability_monitor.h"
#include "stats.h"
#include "trace_helpers.h"
#include "vbucket.h"
#include "vbucket_state.h"

#include <folly/concurrency/UnboundedQueue.h>

#include <memcached/dockey.h>
#include <spdlog/fmt/ostr.h>
#include <statistics/cbstat_collector.h>
#include <utilities/logtags.h>

constexpr std::chrono::milliseconds
        ActiveDurabilityMonitor::State::defaultTimeout;

/**
 * Single-Producer / Single-Consumer Queue of resolved SyncWrites.
 *
 * When a SyncWrite has been resolved (ready to be Committed / Aborted) it is
 * moved from ActiveDM::State::trackedWrites to this class (enqueued).
 *
 * SyncWrites must be completed (produced) in the same order they were tracked,
 * hence there is a single producer, which is enforced by needing to acquire the
 * State::lock when moving items from trackedWrites to the ResolvedQueue;
 * and by recording the highEnqueuedSeqno which must never decrement.
 *
 * SyncWrites must also be committed/aborted (consumed) in-order, as we must
 * enqueue them into the CheckpointManager (where seqnos are assigned) in the
 * same order they were removed from the trackedWrites. This is enforced by
 * a 'consumer' mutex which must be acquired to consume items.
 *
 * Stored separately from State to avoid a potential lock-order-inversion -
 * when SyncWrites are added to State (via addSyncWrite()) the HTLock is
 * acquired before the State lock; however when committing
 * (via seqnoAckReceived()) the State lock must be acquired _before_ HTLock,
 * to be able to determine what actually needs committing. (Similar
 * ordering happens for processTimeout().)
 * Therefore we place the resolved SyncWrites in this queue (while also
 * holding State lock) during seqAckReceived() / processTimeout(); then
 * release the State lock and consume the queue in-order. This ensures
 * that items are removed from this queue (and committed / aborted) in FIFO
 * order.
 */
class ActiveDurabilityMonitor::ResolvedQueue {
public:
    /// Lock which must be acquired to consume (dequeue) items from the queue.
    using ConsumerLock = std::mutex;

    explicit ResolvedQueue(Vbid vbid) {
        highEnqueuedSeqno.setLabel("ActiveDM::ResolvedQueue[" +
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
    void enqueue(const ActiveDurabilityMonitor::State& stateLock,
                 ActiveSyncWrite&& sw) {
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
    folly::Optional<ActiveSyncWrite> try_dequeue(
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

    /**
     * Reset the queue. Locked to prevent races with other consumers. Requires
     * that the queue has already been drained as we shouldn't just allow any
     * reset.
     *
     * @param clg Consumer lock guard which must be acquired to attempt
     *            consumuption (to enforce single consumer).
     */
    void reset(const std::lock_guard<ConsumerLock>& clg) {
        Expects(queue.empty());
        highEnqueuedSeqno.reset(0);
    }

private:
    // Unbounded, Single-producer, single-consumer Queue of ActiveSyncWrite
    // objects, non-blocking variant. Initially holds 2^5 (32) SyncWrites
    using Queue =
            folly::USPSCQueue<DurabilityMonitor::ActiveSyncWrite, false, 5>;
    Queue queue;

    // Track the highest Enqueued Seqno to enforce enqueue ordering. Throws as
    // this could otherwise allow out of order commit on active.
    Monotonic<int64_t, ThrowExceptionPolicy> highEnqueuedSeqno{0};

    /// The lock guarding consumption of items.
    ConsumerLock consumerLock;

    friend std::ostream& operator<<(
            std::ostream& os,
            const ActiveDurabilityMonitor::ResolvedQueue& rq) {
        os << "ResolvedQueue[" << &rq << "] size:" << rq.queue.size()
           << ", highEnqueuedSeqno:" << rq.highEnqueuedSeqno;
        return os;
    }
};

ActiveDurabilityMonitor::ActiveDurabilityMonitor(
        EPStats& stats,
        VBucket& vb,
        std::unique_ptr<EventDrivenDurabilityTimeoutIface> nextExpiryChanged)
    : stats(stats),
      vb(vb),
      state(std::make_unique<State>(*this, std::move(nextExpiryChanged))),
      resolvedQueue(std::make_unique<ResolvedQueue>(vb.getId())) {
}

ActiveDurabilityMonitor::ActiveDurabilityMonitor(
        EPStats& stats,
        VBucket& vb,
        const vbucket_state& vbs,
        std::unique_ptr<EventDrivenDurabilityTimeoutIface> nextExpiryChanged,
        std::vector<queued_item>&& outstandingPrepares)
    : ActiveDurabilityMonitor(stats, vb, std::move(nextExpiryChanged)) {
    if (!vbs.transition.replicationTopology.is_null()) {
        setReplicationTopology(vbs.transition.replicationTopology);
    }
    auto s = state.wlock();
    for (auto& prepare : outstandingPrepares) {
        auto seqno = prepare->getBySeqno();
        // Any outstanding prepares "grandfathered" into the DM from warmup
        // should have an infinite timeout (we cannot abort them as they
        // may already have been Committed before we restarted).
        // (This also means there's no need to consider scheduling the
        // timeout callback).
        Expects(prepare->getDurabilityReqs().getTimeout().isInfinite());
        s->trackedWrites.emplace_back(nullptr,
                                      std::move(prepare),
                                      s->firstChain.get(),
                                      s->secondChain.get(),
                                      ActiveSyncWrite::InfiniteTimeout{});
        s->lastTrackedSeqno = seqno;
    }

    // If we did load sync writes we should get them at least acked for this
    // node, which is achieved by attempting to move the HPS
    s->updateHighPreparedSeqno(*resolvedQueue);

    s->lastTrackedSeqno.reset(vbs.persistedPreparedSeqno);
    s->highPreparedSeqno.reset(vbs.highPreparedSeqno);
    s->highCompletedSeqno.reset(vbs.persistedCompletedSeqno);
}

ActiveDurabilityMonitor::ActiveDurabilityMonitor(
        EPStats& stats,
        VBucket& vb,
        DurabilityMonitor&& dm,
        std::unique_ptr<EventDrivenDurabilityTimeoutIface> nextExpiryChanged)
    : ActiveDurabilityMonitor(stats, vb, std::move(nextExpiryChanged)) {
    EP_LOG_INFO(
            "ActiveDurabilityMonitor::ctor(DM&&): {} Transitioning to ADM. "
            "HPS:{}, HCS:{}, numTracked:{}, highestTracked:{}",
            vb.getId(),
            dm.getHighPreparedSeqno(),
            dm.getHighCompletedSeqno(),
            dm.getNumTracked(),
            dm.getHighestTrackedSeqno());

    int64_t lastSeqno = 0;
#if CB_DEVELOPMENT_ASSERTS
    int64_t lastPreparedSeqno = 0;
    int64_t lastCompletedSeqno = 0;

    uint64_t numberPending = 0;
    uint64_t numberToComplete = 0;
    uint64_t numberCommitted = 0;

    // If true it means that we've received a full snapshot before changing
    // state to ADM from a PDM. Which means all prepare seqnos should come after
    // any committed ones
    const bool snapshotCompleted =
            dm.getHighestTrackedSeqno() <
            static_cast<int64_t>(
                    vb.checkpointManager->getOpenSnapshotStartSeqno());
#endif

    auto s = state.wlock();
    for (auto& write : dm.getTrackedWrites()) {
#if CB_DEVELOPMENT_ASSERTS
        switch (write.getStatus()) {
        case SyncWriteStatus::Pending:
            numberPending++;
            lastPreparedSeqno = write.getBySeqno();
            if (snapshotCompleted) {
                Expects(lastPreparedSeqno > lastCompletedSeqno);
            }
            break;
        case SyncWriteStatus::ToCommit:
        case SyncWriteStatus::ToAbort:
            numberToComplete++;
            break;
        case SyncWriteStatus::Completed:
            numberCommitted++;
            lastCompletedSeqno = write.getBySeqno();
            if (snapshotCompleted) {
                Expects(numberPending == 0);
            }
            break;
        }
#endif
        Expects(write.getBySeqno() > lastSeqno);
        lastSeqno = write.getBySeqno();

        // Any prepares converted from the PDM into the ADM have an infinite
        // timeout set (we cannot abort them as they may already have been
        // Committed when we were non-active.
        // This also means there's no need to consider scheduling the timeout
        // callback here.
        s->trackedWrites.emplace_back(std::move(write));
        Expects(!s->trackedWrites.back().getExpiryTime());
    }

    if (!s->trackedWrites.empty()) {
        s->lastTrackedSeqno = s->trackedWrites.back().getBySeqno();
    } else {
        // If we have no tracked writes then the last tracked should be the last
        // completed. Reset in case we had no SyncWrites (0 -> 0).
        s->lastTrackedSeqno.reset(dm.getHighCompletedSeqno());
    }
    s->highPreparedSeqno.reset(dm.getHighPreparedSeqno());
    s->highCompletedSeqno.reset(dm.getHighCompletedSeqno());
#if CB_DEVELOPMENT_ASSERTS
    if (snapshotCompleted && lastPreparedSeqno > 0) {
        Expects(lastCompletedSeqno < lastPreparedSeqno);
    }
    EP_LOG_INFO(
            "ActiveDurabilityMonitor::ctor(DM&&): finished {} "
            "trackedWrites[numberPending:{}, numberToComplete:{}, "
            "numberCommitted:{}] highPreparedSeqno:{} "
            "highCompletedSeqno:{}",
            vb.getId(),
            numberPending,
            numberToComplete,
            numberCommitted,
            lastPreparedSeqno,
            lastCompletedSeqno);
#endif
}

ActiveDurabilityMonitor::~ActiveDurabilityMonitor() = default;

void ActiveDurabilityMonitor::setReplicationTopology(
        const nlohmann::json& topology) {
    Expects(vb.getState() == vbucket_state_active);
    Expects(!topology.is_null());

    if (!topology.is_array()) {
        throwException<std::invalid_argument>(__func__,
                                              "Topology is not an array");
    }

    if (topology.empty()) {
        throwException<std::invalid_argument>(__func__, "Topology is empty");
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
        s->setReplicationTopology(topology, *resolvedQueue);
    }

    checkForResolvedSyncWrites();
}

int64_t ActiveDurabilityMonitor::getHighPreparedSeqno() const {
    return state.rlock()->highPreparedSeqno;
}

int64_t ActiveDurabilityMonitor::getHighCompletedSeqno() const {
    return state.rlock()->highCompletedSeqno;
}

int64_t ActiveDurabilityMonitor::getHighestTrackedSeqno() const {
    auto s = state.rlock();
    if (!s->trackedWrites.empty()) {
        return s->trackedWrites.back().getBySeqno();
    } else {
        return 0;
    }
}

bool ActiveDurabilityMonitor::isDurabilityPossible() const {
    const auto s = state.rlock();
    // Durability is only possible if we have a first chain for which
    // durability is possible. If we have a second chain, durability must also
    // be possible for that chain.
    return s->firstChain && s->firstChain->isDurabilityPossible() &&
           (!s->secondChain || s->secondChain->isDurabilityPossible());
}

void ActiveDurabilityMonitor::addSyncWrite(const CookieIface* cookie,
                                           queued_item item) {
    auto durReq = item->getDurabilityReqs();

    if (durReq.getLevel() == cb::durability::Level::None) {
        throwException<std::invalid_argument>(__func__, "Level::None");
    }

    // The caller must have already checked this and returned a proper error
    // before executing down here. Here we enforce it again for defending from
    // unexpected races between VBucket::setState (which sets the replication
    // topology).
    if (!isDurabilityPossible()) {
        throwException<std::logic_error>(__func__, "Impossible");
    }

    state.wlock()->addSyncWrite(cookie, std::move(item));
}

cb::engine_errc ActiveDurabilityMonitor::seqnoAckReceived(
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
    //           resolvedQueue (while State and resolvedQueue are both
    //           locked).
    // 2) Lock resolvedQueue, then commit each item and remove from queue.
    //
    // This breaks the potential lock order inversion cycle, as we never acquire
    // both HashBucketLock and State lock together in this function.
    //
    // I don't manage the scenario where step 3 fails yet (note that DM::commit
    // just throws if an error occurs in the current implementation), so this
    // is a @todo.

    // Identify all SyncWrites which are committed by this seqnoAck,
    // transferring them into the resolvedQueue (under the correct locks).
    state.wlock()->processSeqnoAck(replica, preparedSeqno, *resolvedQueue);

    seqnoAckReceivedPostProcessHook();

    // Check if any there's now any resolved SyncWrites which should be
    // completed.
    checkForResolvedSyncWrites();

    return cb::engine_errc::success;
}

void ActiveDurabilityMonitor::processTimeout(
        std::chrono::steady_clock::time_point asOf) {
    // @todo: Add support for DurabilityMonitor at Replica
    if (vb.getState() != vbucket_state_active) {
        throwException<std::logic_error>(
                __func__,
                "state is: " + std::string(VBucket::toString(vb.getState())));
    }

    // Identify SyncWrites which can be timed out as of this time point
    // and should be aborted, transferring them into the completedQueue (under
    // the correct locks).
    state.wlock()->removeExpired(asOf, *resolvedQueue);

    checkForResolvedSyncWrites();
}

void ActiveDurabilityMonitor::notifyLocalPersistence() {
    checkForCommit();
}

void ActiveDurabilityMonitor::addStats(const AddStatFn& addStat,
                                       const CookieIface* cookie) const {
    try {
        const auto vbid = vb.getId().get();

        add_casted_stat(fmt::format("vb_{}:state", vbid),
                        VBucket::toString(vb.getState()),
                        addStat,
                        cookie);

        const auto s = state.rlock();

        add_casted_stat(fmt::format("vb_{}:num_tracked", vbid),
                        s->trackedWrites.size(),
                        addStat,
                        cookie);

        // Do not have a valid HPS unless the first chain has been set.
        int64_t highPreparedSeqno = 0;
        if (s->firstChain) {
            highPreparedSeqno = s->getNodeWriteSeqno(s->getActive());
        }

        add_casted_stat(fmt::format("vb_{}:high_prepared_seqno", vbid),
                        highPreparedSeqno,
                        addStat,
                        cookie);

        add_casted_stat(fmt::format("vb_{}:last_tracked_seqno", vbid),
                        s->lastTrackedSeqno,
                        addStat,
                        cookie);

        add_casted_stat(fmt::format("vb_{}:last_committed_seqno", vbid),
                        s->lastCommittedSeqno,
                        addStat,
                        cookie);

        add_casted_stat(fmt::format("vb_{}:last_aborted_seqno", vbid),
                        s->lastAbortedSeqno,
                        addStat,
                        cookie);

        if (s->firstChain) {
            addStatsForChain(addStat, cookie, *s->firstChain.get());
        }
        if (s->secondChain) {
            addStatsForChain(addStat, cookie, *s->secondChain.get());
        }
    } catch (const std::exception& e) {
        EP_LOG_WARN(
                "({}) ActiveDurabilityMonitor::State:::addStats: error "
                "building stats: {}",
                vb.getId(),
                e.what());
    }
}

void ActiveDurabilityMonitor::addStatsForChain(
        const AddStatFn& addStat,
        const CookieIface* cookie,
        const ReplicationChain& chain) const {
    fmt::memory_buffer buff;
    const auto vbid = vb.getId().get();

    add_casted_stat(fmt::format("vb_{}:replication_chain_{}:size",
                                vbid,
                                to_string(chain.name)),
                    chain.positions.size(),
                    addStat,
                    cookie);

    for (const auto& entry : chain.positions) {
        const auto* node = entry.first.c_str();
        const auto& pos = entry.second;

        add_casted_stat(
                fmt::format("vb_{}:replication_chain_{}:{}:last_write_seqno",
                            vbid,
                            to_string(chain.name),
                            node),
                pos.lastWriteSeqno,
                addStat,
                cookie);

        add_casted_stat(
                fmt::format("vb_{}:replication_chain_{}:{}:last_ack_seqno",
                            vbid,
                            to_string(chain.name),
                            node),
                pos.lastAckSeqno,
                addStat,
                cookie);
    }
}

void ActiveDurabilityMonitor::checkForResolvedSyncWrites() {
    if (resolvedQueue->empty()) {
        return;
    }
    vb.notifySyncWritesPendingCompletion();
}

void ActiveDurabilityMonitor::processCompletedSyncWriteQueue() {
    std::lock_guard<ResolvedQueue::ConsumerLock> lock(
            resolvedQueue->getConsumerLock());
    while (auto sw = resolvedQueue->try_dequeue(lock)) {
        switch (sw->getStatus()) {
        case SyncWriteStatus::Pending:
        case SyncWriteStatus::Completed:
            throw std::logic_error(
                    "ActiveDurabilityMonitor::processCompletedSyncWriteQueue "
                    "found a SyncWrite with unexpected state: " +
                    to_string(sw->getStatus()));
            continue;
        case SyncWriteStatus::ToCommit:
            commit(*sw);
            continue;
        case SyncWriteStatus::ToAbort:
            abort(*sw);
            continue;
        }
        folly::assume_unreachable();
    };
}

void ActiveDurabilityMonitor::unresolveCompletedSyncWriteQueue() {
    // First, remove all of the writes from the resolvedQueue. We should be
    // called from under a WriteHolder of the vBucket state lock so it's safe to
    // release the resolvedQueue consumer lock afterwards.
    Container writesToTrack;
    { // Scope for ResolvedQueue::ConsumerLock
        std::lock_guard<ResolvedQueue::ConsumerLock> lock(
                resolvedQueue->getConsumerLock());
        int64_t lastSeqno = -1;
        while (auto sw = resolvedQueue->try_dequeue(lock)) {
            if (lastSeqno >= sw->getBySeqno()) {
                throw std::logic_error(
                        fmt::format("ActiveDurabilityMonitor::"
                                    "unresolveCompletedSyncWriteQueue(): "
                                    "{} lastSeqno:{} > w.getBySeqno():{} "
                                    "w.getKey():{} {}",
                                    vb.getId(),
                                    lastSeqno,
                                    sw->getBySeqno(),
                                    cb::UserData{sw->getKey().to_string()},
                                    *this));
            }
            lastSeqno = sw->getBySeqno();

            switch (sw->getStatus()) {
            case SyncWriteStatus::Pending:
            case SyncWriteStatus::Completed:
                throw std::logic_error(
                        "ActiveDurabilityMonitor::"
                        "unresolveCompletedSyncWriteQueue "
                        "found a SyncWrite with unexpected state: " +
                        to_string(sw->getStatus()));
            case SyncWriteStatus::ToCommit:
            case SyncWriteStatus::ToAbort:
                // Put our ActiveSyncWrite back into trackedWrites. When we
                // transition to replica we will strip all active only state as
                // required and we need to ensure that our cookie is intact as
                // it will yet be used to respond ambiguous to the client
                writesToTrack.push_back(*sw);
                continue;
            }
        }

        // Reset the resolvedQueue so that if we transition active->dead->active
        // then we do not throw any monotonicity exceptions when completing
        // writes (as the active->dead transition keeps the ADM).
        resolvedQueue->reset(lock);
    }
    // Don't bother taking the state lock if we've got nothing to write to
    // trackedWrites
    if (writesToTrack.empty()) {
        return;
    }

    // Second, whack them back into trackedWrites. The container should be in
    // seqno order so we will just put them at the front of trackedWrites.
    auto s = state.wlock();
    s->trackedWrites.splice(s->trackedWrites.begin(), writesToTrack);

    Expects(resolvedQueue->empty());
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

ActiveDurabilityMonitor::State::State(
        ActiveDurabilityMonitor& adm,
        std::unique_ptr<EventDrivenDurabilityTimeoutIface> nextExpiryChanged)
    : adm(adm), nextExpiryChanged(std::move(nextExpiryChanged)) {
    const auto prefix = "ActiveDM(" + adm.vb.getId().to_string() + ")::State::";
    lastTrackedSeqno.setLabel(prefix + "lastTrackedSeqno");
    lastCommittedSeqno.setLabel(prefix + "lastCommittedSeqno");
    lastAbortedSeqno.setLabel(prefix + "lastAbortedSeqno");
    highPreparedSeqno.setLabel(prefix + "highPreparedSeqno");
    highCompletedSeqno.setLabel(prefix + "highCompletedSeqno");
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
        throwException<std::logic_error>(
                __func__,
                "Attempting to advance positions for an invalid node " + node);
    }

    std::unordered_map<std::string, Position<Container>>::iterator
            secondChainItr;
    auto secondChainFound = false;
    if (secondChain) {
        secondChainItr = secondChain->positions.find(node);
        secondChainFound = secondChainItr != secondChain->positions.end();
        if (!firstChainFound && !secondChainFound) {
            throwException<std::logic_error>(
                    __func__,
                    "Attempting to advance positions for an invalid node " +
                            node +
                            ". Node is not in firstChain or secondChain");
        }
    }

    // Node may be in both chains (or only one) so we need to advance only the
    // correct chain.
    if (firstChainFound) {
        auto& pos = const_cast<Position<Container>&>(firstChainItr->second);
        // We only ack if we do not have this node in the secondChain because
        // we only want to ack once
        advanceAndAckForPosition(pos, node, !secondChainFound /*should ack*/);
        if (!secondChainFound) {
            return pos.it;
        }
    }

    if (secondChainFound) {
        // Update second chain itr
        auto& pos = const_cast<Position<Container>&>(secondChainItr->second);
        advanceAndAckForPosition(pos, node, true /* should ack*/);
        return pos.it;
    }

    folly::assume_unreachable();
}

void ActiveDurabilityMonitor::State::advanceAndAckForPosition(
        Position<Container>& pos, const std::string& node, bool shouldAck) {
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
        const auto event = (node == getActive())
                                   ? cb::tracing::Code::SyncWriteAckLocal
                                   : cb::tracing::Code::SyncWriteAckRemote;
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
        auto& firstChainPos =
                const_cast<Position<Container>&>(firstChainItr->second);
        if (firstChainPos.lastAckSeqno > seqno) {
            EP_LOG_WARN(
                    "({}) Node {} acked seqno:{} lower than previous ack "
                    "seqno:{} "
                    "(first chain)",
                    adm.vb.getId(),
                    node,
                    seqno,
                    int64_t(firstChainPos.lastAckSeqno));
        } else {
            firstChainPos.lastAckSeqno = seqno;
        }
    }

    bool secondChainFound = false;
    if (secondChain) {
        auto secondChainItr = secondChain->positions.find(node);
        if (secondChainItr != secondChain->positions.end()) {
            secondChainFound = true;
            auto& secondChainPos =
                    const_cast<Position<Container>&>(secondChainItr->second);
            if (secondChainPos.lastAckSeqno > seqno) {
                EP_LOG_WARN(
                        "({}) Node {} acked seqno:{} lower than previous ack "
                        "seqno:{} (second chain)",
                        adm.vb.getId(),
                        node,
                        seqno,
                        int64_t(secondChainPos.lastAckSeqno));
            } else {
                secondChainPos.lastAckSeqno = seqno;
            }
        }
    }

    if (!firstChainFound && !secondChainFound) {
        // We didn't find the node in either of our chains, but we still need to
        // track the ack for this node in case we are about to get a topology
        // change in which this node will exist.
        queueSeqnoAck(node, seqno);
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

    throwException<std::invalid_argument>(__func__,
                                          "Node " + node + " not found");
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

    throwException<std::invalid_argument>(__func__,
                                          "Node " + node + " not found");
}

DurabilityMonitor::ActiveSyncWrite
ActiveDurabilityMonitor::State::removeSyncWrite(Container::iterator it,
                                                SyncWriteStatus status) {
    if (it == trackedWrites.end()) {
        throwException<std::logic_error>(__func__, "Position points to end");
    }

    it->setStatus(status);
    // Reset the chains so that we don't attempt to use some possibly re-used
    // memory if we have any bugs that still touch the chains after we remove
    // the SyncWrite from trackedWrites.
    it->resetChains();

    // If we are removing the first element then (a) the "previous" item is
    // different, and (b) we need to re-schedule the SyncWrite timeout task.
    const bool removingFirstElement = it == trackedWrites.begin();

    Container::iterator prev;
    // Note: iterators in trackedWrites are never singular, Container::end
    //     is used as placeholder element for when an iterator cannot point to
    //     any valid element in Container
    if (removingFirstElement) {
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
            const_cast<Position<Container>&>(nodePos).it = prev;
        }
    }

    if (secondChain) {
        for (const auto& entry : secondChain->positions) {
            const auto& nodePos = entry.second;
            if (nodePos.it == it) {
                const_cast<Position<Container>&>(nodePos).it = prev;
            }
        }
    }

    Container removed;
    removed.splice(removed.end(), trackedWrites, it);

    if (removingFirstElement) {
        // If first element was removed, then a new SyncWrite (or possibly none
        // at all) is at the head of trackedWrites and hence now the next
        // SyncWrite to be timed out - reschedule the timeout callback.
        scheduleTimeoutCallback();
    }

    return std::move(removed.front());
}

void ActiveDurabilityMonitor::commit(const ActiveSyncWrite& sw) {
    const auto& key = sw.getKey();
    auto cHandle = vb.lockCollections(key);

    if (!cHandle.valid() || cHandle.isLogicallyDeleted(sw.getBySeqno())) {
        if (sw.getCookie() != nullptr) {
            // collection no longer exists, cannot commit
            vb.notifyClientOfSyncWriteComplete(
                    sw.getCookie(), cb::engine_errc::sync_write_ambiguous);
        }
        return;
    }

    const auto prepareEnd = std::chrono::steady_clock::now();
    auto* cookie = sw.getCookie();
    if (cookie) {
        // Record a Span for the prepare phase duration. We do this before
        // actually calling VBucket::commit() as we want to add a TraceSpan to
        // the cookie before the response to the client is actually sent (and we
        // report the end of the request), which is done within
        // VBucket::commit().
        TracerStopwatch prepareDuration(cookie,
                                        cb::tracing::Code::SyncWritePrepare);
        prepareDuration.start(sw.getStartTime());
        prepareDuration.stop(prepareEnd);
    }
    auto result = vb.commit(key,
                            sw.getBySeqno() /*prepareSeqno*/,
                            {} /*commitSeqno*/,
                            cHandle,
                            sw.getCookie());
    if (result != cb::engine_errc::success) {
        throwException<std::logic_error>(
                __func__, "failed with status: " + cb::to_string(result));
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
        Ensures(s->lastCommittedSeqno <= s->highPreparedSeqno);
    }
}

void ActiveDurabilityMonitor::abort(const ActiveSyncWrite& sw) {
    const auto& key = sw.getKey();

    auto cHandle = vb.lockCollections(key);
    if (!cHandle.valid() || cHandle.isLogicallyDeleted(sw.getBySeqno())) {
        // collection no longer exists, don't generate an abort
        vb.notifyClientOfSyncWriteComplete(
                sw.getCookie(), cb::engine_errc::sync_write_ambiguous);
        return;
    }

    auto result = vb.abort(key,
                           sw.getBySeqno() /*prepareSeqno*/,
                           {} /*abortSeqno*/,
                           cHandle,
                           sw.getCookie());
    if (result != cb::engine_errc::success) {
        throwException<std::logic_error>(
                __func__, "failed with status: " + cb::to_string(result));
    }

    auto s = state.wlock();
    s->lastAbortedSeqno = sw.getBySeqno();
    s->updateHighCompletedSeqno();
    s->totalAborted++;
}

void ActiveDurabilityMonitor::eraseSyncWrite(const DocKey& key, int64_t seqno) {
    auto s = state.wlock();

    // Need to find the write we want to drop
    auto toErase = std::find_if(
            s->trackedWrites.begin(),
            s->trackedWrites.end(),
            [key](const auto& write) -> bool { return write.getKey() == key; });

    // We might call into here with a prepare that does not exist in the DM if
    // the prepare has been completed. We /shouldn't/ do this but it's best to
    // avoid decrementing our iterators if we were to.
    if (toErase == s->trackedWrites.end()) {
        return;
    }

    if (toErase->getBySeqno() != seqno) {
        std::stringstream ss;
        ss << "Attempting to drop prepare for '"
           << cb::tagUserData(key.to_string())
           << "' but seqno does not match. Seqno of prepare: "
           << toErase->getBySeqno() << ", seqno given: " << seqno;
        throwException<std::logic_error>(__func__, "" + ss.str());
    }

    // We need to update the positions for the acks if they are pointing to
    // the writes we are about to erase
    auto valid = toErase == s->trackedWrites.begin() ? s->trackedWrites.end()
                                                     : std::prev(toErase);

    if (s->firstChain) {
        // We really should have a first chain at this point, the only case
        // where we shouldn't should be an upgrade, but better safe than sorry!
        for (auto& position : s->firstChain->positions) {
            if (position.second.it == toErase) {
                position.second.it = valid;
            }
        }
    }

    if (s->secondChain) {
        for (auto& position : s->secondChain->positions) {
            if (position.second.it == toErase) {
                position.second.it = valid;
            }
        }
    }

    // Kick the client so they stop waiting for a response. Not doing so would
    // block shutdowns.
    auto cookie = toErase->getCookie();

    // Might lose the cookie on state transitions so can't assume it's there.
    if (cookie) {
        vb.notifyClientOfSyncWriteComplete(
                cookie, cb::engine_errc::sync_write_ambiguous);
    }

    // And erase
    s->trackedWrites.erase(toErase);
}

std::vector<const CookieIface*>
ActiveDurabilityMonitor::prepareTransitionAwayFromActive() {
    // Put everything in the resolvedQueue back into trackedWrites. This is
    // necessary as we may have decided to resolve something that our new active
    // will try to send us a commit for and we need to have the prepare in
    // trackedWrites to deal with that
    unresolveCompletedSyncWriteQueue();

    // Return the cookies so that the caller can respond to all of your clients
    // with ambiguous (in a background task)
    return getCookiesForInFlightSyncWrites();
}

std::vector<const CookieIface*>
ActiveDurabilityMonitor::getCookiesForInFlightSyncWrites() {
    auto vec = std::vector<const CookieIface*>();

    std::lock_guard<ResolvedQueue::ConsumerLock> lock(
            resolvedQueue->getConsumerLock());
    // Take a write lock on the state now as we don't want trackedWrites being
    // completed and being placed on the resolvedQueue while we pop all the
    // SyncWrites from the resolvedQueue and then re-push them.
    auto s = state.wlock();
    std::vector<ActiveSyncWrite> resolvedSwToBeRePushed;
    while (auto write = resolvedQueue->try_dequeue(lock)) {
        auto* cookie = write->getCookie();
        if (cookie) {
            vec.push_back(cookie);
            write->clearCookie();
        }
        resolvedSwToBeRePushed.push_back(*write);
    }
    // "reset" the queue, this just sets the value of highEnqueuedSeqno back
    // to 0 and ensures that the queue is empty. No memory management is
    // performed by the method.
    resolvedQueue->reset(lock);
    for (auto& sw : resolvedSwToBeRePushed) {
        resolvedQueue->enqueue(*s, std::move(sw));
    }

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
                                                     ResolvedQueue& toCommit) {
    if (!firstChain) {
        // MB-37188: Tests have demonstrated that during an upgrade to 6.5.0,
        // once all nodes are upgraded and the DCP streams are recreated to flip
        // to support sync replication, a seqno ack may be received prior to the
        // topology being set. This occurs because the HPS will be moved at the
        // end of a disk snapshot even in the absence of prepares (there cannot
        // be any existing prepares from before the upgrade as 6.5.0 introduces
        // durability).
        // Queue the ack for processing once a topology is received.
        queueSeqnoAck(node, seqno);
        return;
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
            Expects(posIt->getStatus() == SyncWriteStatus::Pending);
            toCommit.enqueue(*this,
                             removeSyncWrite(posIt, SyncWriteStatus::ToCommit));
        }
    }

    // We keep track of the actual ack'ed seqno
    updateNodeAck(node, seqno);
}

std::list<DurabilityMonitor::SyncWrite>
ActiveDurabilityMonitor::getTrackedWrites() const {
    auto s = state.rlock();
    std::list<DurabilityMonitor::SyncWrite> ret;
    for (auto& write : s->trackedWrites) {
        ret.emplace_back(write);
    }
    return ret;
}

std::unordered_set<int64_t> ActiveDurabilityMonitor::getTrackedSeqnos() const {
    const auto s = state.rlock();
    std::unordered_set<int64_t> ret;
    for (const auto& w : s->trackedWrites) {
        ret.insert(w.getBySeqno());
    }
    return ret;
}

std::vector<StoredDocKey> ActiveDurabilityMonitor::getTrackedKeys() const {
    std::vector<StoredDocKey> items;
    auto s = state.rlock();
    for (auto& w : s->trackedWrites) {
        items.push_back(w.getKey());
    }
    return items;
}

void ActiveDurabilityMonitor::dump() const {
    toOStream(std::cerr);
}

void ActiveDurabilityMonitor::toOStream(std::ostream& os) const {
    os << "ActiveDurabilityMonitor[" << this << "] " << *state.rlock();
    os << "resolvedQueue: " << *resolvedQueue << "\n";
}

void ActiveDurabilityMonitor::chainToOstream(
        std::ostream& os,
        const ReplicationChain& rc,
        Container::const_iterator trackedWritesEnd) {
    os << "Chain[" << &rc << "] name:" << to_string(rc.name)
       << " majority:" << int(rc.majority) << " active:" << rc.active
       << " maxAllowedReplicas:" << rc.maxAllowedReplicas << " positions:[\n";
    for (const auto& pos : rc.positions) {
        os << "    " << pos.first << ": "
           << to_string(pos.second, trackedWritesEnd) << "\n";
    }
    os << "]";
}

void ActiveDurabilityMonitor::validateChain(
        const nlohmann::json& chain,
        DurabilityMonitor::ReplicationChainName chainName) {
    if (chain.empty()) {
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

std::unique_ptr<ActiveDurabilityMonitor::ReplicationChain>
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
            name,
            nodes,
            trackedWrites.end(),
            adm.vb.maxAllowedReplicasForSyncWrites);

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
            throwException<std::logic_error>(
                    __func__,
                    "did not find the "
                    "active node for the first chain in the "
                    "first chain.");
        }

        auto newChainItr = ptr->positions.find(ptr->active);
        if (newChainItr == ptr->positions.end()) {
            // Sanity - we should never make a chain in this state
            throwException<std::logic_error>(
                    __func__,
                    "did not find the "
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
        const nlohmann::json& topology, ResolvedQueue& toComplete) {
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
            throwException<std::invalid_argument>(__func__,
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

    // Apply the new topology to all in-flight SyncWrites.
    for (auto& write : trackedWrites) {
        write.resetTopology(*newFirstChain, newSecondChain.get());
    }

    // Set the HPS correctly if we are transitioning from a null topology (may
    // be in-flight SyncWrites from a PDM that we use to do this). Must be done
    // after we have have set the topology of the SyncWrites or they will have
    // no chain.
    if (!firstChain) {
        transitionFromNullTopology(*newFirstChain);
    }

    // We have already reset the topology of the in flight SyncWrites so that
    // they do not contain any invalid pointers to ReplicationChains post
    // topology change.
    abortNoLongerPossibleSyncWrites(
            *newFirstChain, newSecondChain.get(), toComplete);

    // Copy the iterators from the old chains to the new chains.
    copyChainPositions(firstChain.get(),
                       *newFirstChain,
                       secondChain.get(),
                       newSecondChain.get());

    // We have now reset all the topology for SyncWrites so we can dispose of
    // the old chain (by overwriting it with the new one).
    firstChain = std::move(newFirstChain);
    secondChain = std::move(newSecondChain);

    // Manually ack any nodes that did not previously exist in either chain
    performQueuedAckForChain(*firstChain, toComplete);

    if (secondChain) {
        performQueuedAckForChain(*secondChain, toComplete);
    }

    // Commit if possible
    cleanUpTrackedWritesPostTopologyChange(toComplete);
}

void ActiveDurabilityMonitor::State::transitionFromNullTopology(
        ReplicationChain& newFirstChain) {
    if (!trackedWrites.empty()) {
        // We need to manually set the values for the HPS iterator
        // (newFirstChain->positions.begin()) and "ack" the nodes so that we
        // can commit if possible by checking if they are satisfied.

        // It may be the case that we had a PersistToMajority prepare in the
        // PDM before moving to ADM that had not yet been persisted
        // (trackedWrites.back().getBySeqno() != highPreparedSeqno). If we
        // have persisted this prepare in between transitioning from PDM
        // to ADM with null topology and transitioning from ADM with null
        // topology to ADM with topology then we may need to move our HPS
        // further than the highPreparedSeqno that we inherited from the PDM
        // due to persistence.
        auto fence = std::max(static_cast<uint64_t>(highPreparedSeqno),
                              adm.vb.getPersistenceSeqno());
        auto& activePos =
                newFirstChain.positions.find(newFirstChain.active)->second;
        auto it = trackedWrites.begin();
        while (it != trackedWrites.end()) {
            if (it->getBySeqno() <= static_cast<int64_t>(fence)) {
                activePos.it = it;
                it->ack(newFirstChain.active);
                it = std::next(it);
            } else {
                break;
            }
        }

        activePos.lastWriteSeqno = static_cast<int64_t>(fence);
        highPreparedSeqno = static_cast<int64_t>(fence);
    }
}

void ActiveDurabilityMonitor::State::copyChainPositions(
        ReplicationChain* oldFirstChain,
        ReplicationChain& newFirstChain,
        ReplicationChain* oldSecondChain,
        ReplicationChain* newSecondChain) {
    if (oldFirstChain) {
        // Copy over the trackedWrites position for all nodes which still exist
        // in the new chain. This ensures that if we manually set the HPS on the
        // firstChain then the secondChain will also be correctly set.
        copyChainPositionsInner(*oldFirstChain, newFirstChain);
        if (newSecondChain) {
            // This stage should never matter because we will find the node in
            // the firstChain and return early from processSeqnoAck. Added for
            // the sake of completeness.
            // @TODO make iterators optional and remove this
            copyChainPositionsInner(*oldFirstChain, *newSecondChain);
        }
    }

    if (oldSecondChain) {
        copyChainPositionsInner(*oldSecondChain, newFirstChain);
        if (newSecondChain) {
            copyChainPositionsInner(*oldSecondChain, *newSecondChain);
        }
    }
}

void ActiveDurabilityMonitor::State::copyChainPositionsInner(
        ReplicationChain& oldChain, ReplicationChain& newChain) {
    for (const auto& node : oldChain.positions) {
        auto newNode = newChain.positions.find(node.first);
        if (newNode != newChain.positions.end()) {
            newNode->second = node.second;
        }
    }
}

void ActiveDurabilityMonitor::State::abortNoLongerPossibleSyncWrites(
        ReplicationChain& newFirstChain,
        ReplicationChain* newSecondChain,
        ResolvedQueue& toAbort) {
    // If durability is not possible for the new chains, then we should abort
    // any in-flight SyncWrites that do not have an infinite timeout so that the
    // client can decide what to do. We do not abort and infinite timeout
    // SyncWrites as we MUST complete them as they exist due to a warmup or
    // Passive->Active transition. We have already reset the topology of the in
    // flight SyncWrites so that they do not contain any invalid pointers post
    // topology change.
    if (!(newFirstChain.isDurabilityPossible() &&
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
                toAbort.enqueue(*this,
                                removeSyncWrite(trackedWrites.begin(),
                                                SyncWriteStatus::ToAbort));
                itr = next;
            } else {
                itr++;
            }
        }
    }
}

void ActiveDurabilityMonitor::State::performQueuedAckForChain(
        const ActiveDurabilityMonitor::ReplicationChain& chain,
        ResolvedQueue& toCommit) {
    for (const auto& node : chain.positions) {
        auto existingAck = queuedSeqnoAcks.find(node.first);
        if (existingAck != queuedSeqnoAcks.end()) {
            processSeqnoAck(existingAck->first, existingAck->second, toCommit);

            // Remove the existingAck, we don't need to track it any further as
            // it is in a chain.
            queuedSeqnoAcks.erase(existingAck);
        }
    }
}

void ActiveDurabilityMonitor::State::queueSeqnoAck(const std::string& node,
                                                   int64_t seqno) {
    queuedSeqnoAcks[node] = seqno;
    queuedSeqnoAcks[node].setLabel("queuedSeqnoAck: " + node);
}

void ActiveDurabilityMonitor::State::cleanUpTrackedWritesPostTopologyChange(
        ActiveDurabilityMonitor::ResolvedQueue& toCommit) {
    auto it = trackedWrites.begin();
    while (it != trackedWrites.end()) {
        const auto next = std::next(it);
        // Remove from trackedWrites anything that is completed. This may happen
        // if we have been created from a PDM that has not received a full
        // snapshot. We have to do this after we set the HPS otherwise we could
        // end up with an ADM with lower HPS than the previous PDM.
        if (it->isCompleted()) {
            removeSyncWrite(it, SyncWriteStatus::Completed);
        } else if (it->isSatisfied()) {
            toCommit.enqueue(*this,
                             removeSyncWrite(it, SyncWriteStatus::ToCommit));
        }
        it = next;
    }
}

void ActiveDurabilityMonitor::State::addSyncWrite(const CookieIface* cookie,
                                                  queued_item item) {
    Expects(firstChain.get());
    const auto seqno = item->getBySeqno();
    const auto wasEmpty = trackedWrites.empty();
    trackedWrites.emplace_back(cookie,
                               std::move(item),
                               defaultTimeout,
                               firstChain.get(),
                               secondChain.get());

    if (wasEmpty) {
        // trackedWrites transitioned from empty to non-empty; so the front
        // item has changed (we now have one) and hence the next timeout
        // callback should be scheduled.
        scheduleTimeoutCallback();
    }

    lastTrackedSeqno = seqno;
    totalAccepted++;
}

void ActiveDurabilityMonitor::State::removeExpired(
        std::chrono::steady_clock::time_point asOf, ResolvedQueue& expired) {
    // Given SyncWrites must complete In-Order, iterate from the beginning
    // of trackedWrites only as long as we find expired items; if we encounter
    // any unexpired items then must stop.
    auto it = trackedWrites.begin();
    while (it != trackedWrites.end()) {
        if (it->isExpired(asOf)) {
            // Note: 'it' will be invalidated, so it will need to be reset
            const auto next = std::next(it);

            expired.enqueue(*this,
                            removeSyncWrite(it, SyncWriteStatus::ToAbort));

            it = next;
        } else {
            // Encountered an unexpired item - must stop.
            break;
        }
    }
}

void ActiveDurabilityMonitor::State::scheduleTimeoutCallback() {
    if (!trackedWrites.empty()) {
        const auto nextExpiry = trackedWrites.front().getExpiryTime();
        if (nextExpiry) {
            nextExpiryChanged->updateNextExpiryTime(*nextExpiry);
            return;
        }
    }
    // No SyncWrites exist, or no expiry set - cancel expiry task.
    nextExpiryChanged->cancelNextExpiryTime();
}

void ActiveDurabilityMonitor::State::updateHighPreparedSeqno(
        ResolvedQueue& completed) {
    // Note: All the logic below relies on the fact that HPS for Active is
    //     implicitly the tracked position for Active in FirstChain

    if (trackedWrites.empty()) {
        return;
    }

    if (!firstChain) {
        // An ActiveDM _may_ legitimately have no topology information, if
        // for example it has just been created from a PassiveDM during takeover
        // and ns_server has not yet updated the VBucket's topology.
        // In this case, it may be possible to update the HPS and we should do
        // so to ensure that any subsequent state change back to
        // replica/PassiveDM acks correctly if we never got a topology. We can
        // update the highPreparedSeqno for anything that the PDM completed
        // (we should have nothing in trackedWrites not completed as we have no
        // topology) by using the store value instead of the iterator. Given
        // we only keep these completed SyncWrites in trackedWrites to correctly
        // set the HPS when we DO get a topology, we can remove them once we
        // have advanced past them.
        auto itr = trackedWrites.begin();
        while (itr != trackedWrites.end()) {
            if (!itr->isCompleted()) {
                return;
            }

            // Don't advance past anything not persisted.
            auto level = itr->getDurabilityReqs().getLevel();
            if ((level == cb::durability::Level::PersistToMajority ||
                 level == cb::durability::Level::MajorityAndPersistOnMaster) &&
                static_cast<uint64_t>(itr->getBySeqno()) <
                        adm.vb.getPersistenceSeqno()) {
                return;
            }

            highPreparedSeqno = itr->getBySeqno();

            auto next = std::next(itr);
            trackedWrites.erase(itr);
            itr = next;
        }
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
            completed.enqueue(
                    *this, removeSyncWrite(pos.it, SyncWriteStatus::ToCommit));
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
    try {
        highCompletedSeqno = std::max(lastCommittedSeqno, lastAbortedSeqno);
    } catch (const std::exception& e) {
        EP_LOG_ERR(
                "ActiveDurabilityMonitor::State::updateHighCompletedSeqno(): "
                "threw:{} {} ActiveDurabilityMonitor[{}] state:{} "
                "resolvedQueue:{}",
                e.what(),
                adm.vb.getId(),
                (void*)&adm,
                *this,
                std::ref(*adm.resolvedQueue));
        throw;
    }
}

void ActiveDurabilityMonitor::State::dump() const {
    std::cerr << *this;
}

void ActiveDurabilityMonitor::checkForCommit() {
    // Identify all SyncWrites which are now committed, transferring them into
    // the resolvedQueue (under the correct locks).
    state.wlock()->updateHighPreparedSeqno(*resolvedQueue);

    checkForResolvedSyncWrites();
}

template <class exception>
[[noreturn]] void ActiveDurabilityMonitor::State::throwException(
        const std::string& thrower, const std::string& error) const {
    throw exception("ActiveDurabilityMonitor::State::" + thrower + " " +
                    adm.vb.getId().to_string() + " " + error);
}

template <class exception>
[[noreturn]] void ActiveDurabilityMonitor::throwException(
        const std::string& thrower, const std::string& error) const {
    throw exception("ActiveDurabilityMonitor::" + thrower + " " +
                    vb.getId().to_string() + " " + error);
}
