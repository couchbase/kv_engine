/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2018 Couchbase, Inc.
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

#include "durability_monitor.h"
#include "item.h"
#include "monotonic.h"
#include "stored-value.h"
#include "vbucket.h"

#include <gsl.h>
#include <unordered_map>

/*
 * Represents the tracked state of a replica.
 * Note that the lifetime of a Position is determined by the logic in
 * DurabilityMonitor.
 *
 * - it: Iterator that points to a position in the Container of tracked
 *         SyncWrites. This is an optimization: logically it points always to
 *         the last SyncWrite acknowledged by the tracked replica, so that we
 *         can avoid any O(N) scan when updating the replica state at seqno-ack
 *         received. It may point to Container::end (e.g, when the pointed
 *         SyncWrite is the last element in Container and it is removed).
 *
 * - lastSyncWriteSeqno: Stores always the seqno of the last SyncWrite
 *         acknowledged by the tracked replica, even when Position::it points to
 *         Container::end. Used for validation at seqno-ack received and stats.
 *
 * - lastAckSeqno: Stores always the last seqno acknowledged by the tracked
 *         replica. Used for validation at seqno-ack received and stats.
 */
struct DurabilityMonitor::Position {
    Container::iterator it;
    Monotonic<int64_t> lastSyncWriteSeqno;
    Monotonic<int64_t> lastAckSeqno;
};

/*
 * Represents a tracked SyncWrite.
 */
class DurabilityMonitor::SyncWrite {
public:
    SyncWrite(const void* cookie, queued_item item)
        : item(item), cookie(cookie) {
    }

    const StoredDocKey& getKey() const {
        return item->getKey();
    }

    int64_t getBySeqno() const {
        return item->getBySeqno();
    }

    cb::durability::Requirements getDurabilityReqs() const {
        return item->getDurabilityReqs();
    }

    const void* getCookie() const {
        return cookie;
    }

private:
    // An Item stores all the info that the DurabilityMonitor needs:
    // - seqno
    // - Durability Requirements
    // Note that queued_item is a ref-counted object, so the copy in the
    // CheckpointManager can be safely removed.
    const queued_item item;

    // Client cookie associated with this SyncWrite request, to be notified
    // when the SyncWrite completes.
    const void* cookie;
};

/*
 * Represents a VBucket Replication Chain in the ns_server meaning,
 * i.e. a list of replica nodes where the VBucket replicas reside.
 */
struct DurabilityMonitor::ReplicationChain {
    /**
     * @param nodes ns_server-like set of replica ids, eg:
     *     {replica1, replica2, ..}
     */
    ReplicationChain(const std::vector<std::string>& nodes,
                     const Container::iterator& it)
        : majority(nodes.size() / 2 + 1) {
        for (auto node : nodes) {
            memoryPositions[node] = {it, 0 /*lastSeqno*/, 0 /*lastAckSeqno*/};
        }
    }

    // Index of replica Positions. The key is the replica id.
    // A Position embeds the in-memory state of the tracked replica.
    std::unordered_map<std::string, Position> memoryPositions;

    // Majority in the arithmetic definition: NumReplicas / 2 + 1
    const uint8_t majority;
};

DurabilityMonitor::DurabilityMonitor(VBucket& vb) : vb(vb) {
}

DurabilityMonitor::~DurabilityMonitor() = default;

ENGINE_ERROR_CODE DurabilityMonitor::registerReplicationChain(
        const std::vector<std::string>& nodes) {
    if (nodes.size() == 0) {
        throw std::logic_error(
                "DurabilityMonitor::registerReplicationChain: empty chain not "
                "allowed");
    }

    if (nodes.size() > 1) {
        return ENGINE_ENOTSUP;
    }

    // Statically create a single RC. This will be expanded for creating
    // multiple RCs dynamically.
    std::lock_guard<std::mutex> lg(state.m);
    state.firstChain = std::make_unique<ReplicationChain>(
            nodes, state.trackedWrites.begin());

    return ENGINE_SUCCESS;
}

ENGINE_ERROR_CODE DurabilityMonitor::addSyncWrite(const void* cookie,
                                                  queued_item item) {
    auto durReq = item->getDurabilityReqs();
    if (durReq.getLevel() != cb::durability::Level::Majority ||
        durReq.getTimeout() != 0) {
        return ENGINE_ENOTSUP;
    }

    std::lock_guard<std::mutex> lg(state.m);
    if (!state.firstChain) {
        throw std::logic_error(
                "DurabilityMonitor::addSyncWrite: no chain registered");
    }
    state.trackedWrites.push_back(SyncWrite(cookie, item));

    return ENGINE_SUCCESS;
}

ENGINE_ERROR_CODE DurabilityMonitor::seqnoAckReceived(
        const std::string& replica, int64_t memorySeqno) {
    // Note:
    // TSan spotted that in the execution path to DM::addSyncWrites we acquire
    // HashBucketLock first and then a lock to DM::state.m, while here we
    // acquire first the lock to state.m and then HashBucketLock.
    // This could cause a deadlock by lock inversion (note that the 2 execution
    // paths are expected to execute in 2 different threads).
    // Given that the HashBucketLock here is acquired in the sub-call to
    // VBucket::commit, then to fix I need to release the lock to state.m
    // before executing DM::commit.
    //
    // By logic the correct order of processing for every verified SyncWrite
    // would be:
    // 1) check if DurabilityRequirements are satisfied
    // 2) if they are, then commit
    // 3) remove the committed SyncWrite from tracking
    //
    // But, we are in the situation where steps 1 and 3 must execute under lock
    // to state.m, while step 2 must not.
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
    {
        std::lock_guard<std::mutex> lg(state.m);

        if (!state.firstChain) {
            throw std::logic_error(
                    "DurabilityMonitor::seqnoAckReceived: no chain registered");
        }

        if (state.trackedWrites.empty()) {
            throw std::logic_error(
                    "DurabilityManager::seqnoAckReceived: No tracked "
                    "SyncWrite, "
                    "but replica ack'ed memorySeqno:" +
                    std::to_string(memorySeqno));
        }

        auto next = getReplicaMemoryNext(lg, replica);

        if (next == state.trackedWrites.end()) {
            throw std::logic_error(
                    "DurabilityManager::seqnoAckReceived: No pending "
                    "SyncWrite, "
                    "but replica ack'ed memorySeqno:" +
                    std::to_string(memorySeqno));
        }

        int64_t pendingSeqno = next->getBySeqno();
        if (memorySeqno < pendingSeqno) {
            throw std::logic_error(
                    "DurabilityManager::seqnoAckReceived: Ack'ed seqno is "
                    "behind "
                    "pending seqno {ack'ed: " +
                    std::to_string(memorySeqno) +
                    ", pending:" + std::to_string(pendingSeqno) + "}");
        }

        do {
            // Update replica tracking
            advanceReplicaMemoryPosition(lg, replica);

            // Note: In this first implementation (1 replica) the Durability
            // Requirement for the pending SyncWrite is implicitly verified
            // at this point.

            toCommit.splice(
                    toCommit.end(),
                    removeSyncWrite(
                            lg, state.firstChain->memoryPositions.at(replica)));

            // Note: process up to the ack'ed memory seqno
        } while ((next = getReplicaMemoryNext(lg, replica)) !=
                         state.trackedWrites.end() &&
                 next->getBySeqno() <= memorySeqno);

        // We keep track of the actual ack'ed seqno
        updateReplicaMemoryAckSeqno(lg, replica, memorySeqno);
    }

    // Commit the verified SyncWrites
    for (const auto& entry : toCommit) {
        commit(entry.getKey(), entry.getBySeqno(), entry.getCookie());
    }

    return ENGINE_SUCCESS;
}

size_t DurabilityMonitor::getNumTracked(
        const std::lock_guard<std::mutex>& lg) const {
    return state.trackedWrites.size();
}

size_t DurabilityMonitor::getReplicationChainSize(
        const std::lock_guard<std::mutex>& lg) const {
    return state.firstChain->memoryPositions.size();
}

DurabilityMonitor::Container::iterator DurabilityMonitor::getReplicaMemoryNext(
        const std::lock_guard<std::mutex>& lg, const std::string& replica) {
    const auto& it = state.firstChain->memoryPositions.at(replica).it;
    // Note: Container::end could be the new position when the pointed SyncWrite
    //     is removed from Container and the iterator repositioned.
    //     In that case next=Container::begin
    return (it == state.trackedWrites.end()) ? state.trackedWrites.begin()
                                             : std::next(it);
}

void DurabilityMonitor::advanceReplicaMemoryPosition(
        const std::lock_guard<std::mutex>& lg, const std::string& replica) {
    auto& pos = state.firstChain->memoryPositions.at(replica);
    pos.it++;
    // Note that Position::lastSyncWriteSeqno is always set to the current
    // pointed SyncWrite to keep the replica seqno-state for when the pointed
    // SyncWrite is removed
    pos.lastSyncWriteSeqno = pos.it->getBySeqno();
}

void DurabilityMonitor::updateReplicaMemoryAckSeqno(
        const std::lock_guard<std::mutex>& lg,
        const std::string& replica,
        int64_t seqno) {
    auto& pos = state.firstChain->memoryPositions.at(replica);
    pos.lastAckSeqno = seqno;
}

int64_t DurabilityMonitor::getReplicaMemorySyncWriteSeqno(
        const std::lock_guard<std::mutex>& lg,
        const std::string& replica) const {
    return state.firstChain->memoryPositions.at(replica).lastSyncWriteSeqno;
}

int64_t DurabilityMonitor::getReplicaMemoryAckSeqno(
        const std::lock_guard<std::mutex>& lg,
        const std::string& replica) const {
    return state.firstChain->memoryPositions.at(replica).lastAckSeqno;
}

DurabilityMonitor::Container DurabilityMonitor::removeSyncWrite(
        const std::lock_guard<std::mutex>& lg, const Position& pos) {
    if (pos.it == state.trackedWrites.end()) {
        throw std::logic_error(
                "DurabilityMonitor::commit: Position points to end");
    }

    // Note that Position.seqno stays set to the original value. That way we
    // keep the replica seqno-state even after the SyncWrite is removed.
    auto committedSeqno = pos.lastSyncWriteSeqno;
    Container::iterator prev;
    // Note: iterators in state.trackedWrites are never singular, Container::end
    //     is used as placeholder element for when an iterator cannot point to
    //     any valid element in Container
    if (pos.it == state.trackedWrites.begin()) {
        prev = state.trackedWrites.end();
    } else {
        prev = std::prev(pos.it);
    }

    auto& pos_ = const_cast<Position&>(pos);
    Container removed;
    removed.splice(removed.end(), state.trackedWrites, pos_.it);
    pos_.it = prev;

    Ensures(pos_.lastSyncWriteSeqno == committedSeqno);

    return removed;
}

void DurabilityMonitor::commit(const StoredDocKey& key,
                               int64_t seqno,
                               const void* cookie) {
    // The next call:
    // 1) converts the SyncWrite in the HashTable from Prepare to Committed
    // 2) enqueues a Commit SyncWrite item into the CheckpointManager
    auto result = vb.commit(key, seqno, {});
    if (result != ENGINE_SUCCESS) {
        throw std::logic_error(
                "DurabilityMonitor::commit: VBucket::commit failed with "
                "status:" +
                std::to_string(result));
    }

    // 3) send a response with Success back to the client
    vb.notifyClientOfCommit(cookie);
}
