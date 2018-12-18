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
    SyncWrite(queued_item item) : item(item) {
    }

    int64_t getBySeqno() const {
        return item->getBySeqno();
    }

    cb::durability::Requirements getDurabilityReqs() const {
        return item->getDurabilityReqs();
    }

private:
    // An Item stores all the info that the DurabilityMonitor needs:
    // - seqno
    // - Durability Requirements
    // Note that queued_item is a ref-counted object, so the copy in the
    // CheckpointManager can be safely removed.
    const queued_item item;
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

ENGINE_ERROR_CODE DurabilityMonitor::addSyncWrite(queued_item item) {
    auto durReq = item->getDurabilityReqs();
    if (durReq.getLevel() != cb::durability::Level::Majority ||
        durReq.getTimeout() != 0) {
        return ENGINE_ENOTSUP;
    }
    std::lock_guard<std::mutex> lg(state.m);
    state.trackedWrites.push_back(SyncWrite(item));
    return ENGINE_SUCCESS;
}

ENGINE_ERROR_CODE DurabilityMonitor::seqnoAckReceived(
        const std::string& replica, int64_t memorySeqno) {
    std::lock_guard<std::mutex> lg(state.m);

    if (state.trackedWrites.empty()) {
        throw std::logic_error(
                "DurabilityManager::seqnoAckReceived: No tracked SyncWrite, "
                "but replica ack'ed memorySeqno:" +
                std::to_string(memorySeqno));
    }

    if (!hasPending(lg, replica)) {
        throw std::logic_error(
                "DurabilityManager::seqnoAckReceived: No pending SyncWrite, "
                "but replica ack'ed memorySeqno:" +
                std::to_string(memorySeqno));
    }

    int64_t pendingSeqno = getReplicaPendingMemorySeqno(lg, replica);
    if (memorySeqno < pendingSeqno) {
        throw std::logic_error(
                "DurabilityManager::seqnoAckReceived: Ack'ed seqno is behind "
                "pending seqno {ack'ed: " +
                std::to_string(memorySeqno) +
                ", pending:" + std::to_string(pendingSeqno) + "}");
    }

    while (hasPending(lg, replica)) {
        // Process up to the ack'ed memory seqno
        if (getReplicaPendingMemorySeqno(lg, replica) > memorySeqno) {
            break;
        }

        // Update replica tracking
        advanceReplicaMemoryPosition(lg, replica);

        // Note: In this first implementation (1 replica) the Durability
        // Requirement for the pending SyncWrite is implicitly verified
        // at this point.

        // Commit the verified SyncWrite
        commit(lg, getReplicaMemoryPosition(lg, replica));
    }

    // We keep track of the actual ack'ed seqno
    updateReplicaMemoryAckSeqno(lg, replica, memorySeqno);

    return ENGINE_SUCCESS;
}

size_t DurabilityMonitor::getNumTracked(
        const std::lock_guard<std::mutex>& lg) const {
    return state.trackedWrites.size();
}

const DurabilityMonitor::Position& DurabilityMonitor::getReplicaMemoryPosition(
        const std::lock_guard<std::mutex>& lg,
        const std::string& replica) const {
    if (!state.firstChain) {
        throw std::logic_error(
                "DurabilityMonitor::getReplicaMemoryIterator: no chain "
                "registered");
    }
    const auto entry = state.firstChain->memoryPositions.find(replica);
    if (entry == state.firstChain->memoryPositions.end()) {
        throw std::invalid_argument(
                "DurabilityMonitor::getReplicaEntry: replica " + replica +
                " not found in chain");
    }
    return entry->second;
}

DurabilityMonitor::Container::iterator DurabilityMonitor::getReplicaMemoryNext(
        const std::lock_guard<std::mutex>& lg, const std::string& replica) {
    const auto& it = getReplicaMemoryPosition(lg, replica).it;
    // Note: Container::end could be the new position when the pointed SyncWrite
    //     is removed from Container and the iterator repositioned.
    //     In that case next=Container::begin
    return (it == state.trackedWrites.end()) ? state.trackedWrites.begin()
                                             : std::next(it);
}

void DurabilityMonitor::advanceReplicaMemoryPosition(
        const std::lock_guard<std::mutex>& lg, const std::string& replica) {
    auto& pos = const_cast<Position&>(getReplicaMemoryPosition(lg, replica));
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
    auto& pos = const_cast<Position&>(getReplicaMemoryPosition(lg, replica));
    pos.lastAckSeqno = seqno;
}

int64_t DurabilityMonitor::getReplicaMemorySyncWriteSeqno(
        const std::lock_guard<std::mutex>& lg,
        const std::string& replica) const {
    return getReplicaMemoryPosition(lg, replica).lastSyncWriteSeqno;
}

int64_t DurabilityMonitor::getReplicaMemoryAckSeqno(
        const std::lock_guard<std::mutex>& lg,
        const std::string& replica) const {
    return getReplicaMemoryPosition(lg, replica).lastAckSeqno;
}

bool DurabilityMonitor::hasPending(const std::lock_guard<std::mutex>& lg,
                                   const std::string& replica) {
    return getReplicaMemoryNext(lg, replica) != state.trackedWrites.end();
}

int64_t DurabilityMonitor::getReplicaPendingMemorySeqno(
        const std::lock_guard<std::mutex>& lg, const std::string& replica) {
    const auto& next = getReplicaMemoryNext(lg, replica);
    if (next == state.trackedWrites.end()) {
        return 0;
    }
    return next->getBySeqno();
}

void DurabilityMonitor::commit(const std::lock_guard<std::mutex>& lg,
                               const Position& pos) {
    if (pos.it == state.trackedWrites.end()) {
        throw std::logic_error(
                "DurabilityMonitor::commit: Position points to end");
    }

    // @todo: do commit.
    // Here we will:
    // 1) update the SyncWritePrepare to SyncWriteCommit in the HT
    // 2) enqueue a SyncWriteCommit item into the CM
    // 3) send a response with Success back to the client

    // Remove the committed SyncWrite.
    // Note that Position.seqno stays set to the original value. That way we
    // keep the replica seqno-state even after the SyncWrite is removed.
    auto committedSeqno = pos.lastSyncWriteSeqno;
    auto& pos_ = const_cast<Position&>(pos);
    Container::iterator prev;
    // Note: iterators in state.trackedWrites are never singular, Container::end
    //     is used as placeholder element for when an iterator cannot point to
    //     any valid element in Container
    if (pos_.it == state.trackedWrites.begin()) {
        prev = state.trackedWrites.end();
    } else {
        prev = std::prev(pos_.it);
    }
    state.trackedWrites.erase(pos_.it);
    pos_.it = prev;

    Ensures(pos_.lastSyncWriteSeqno == committedSeqno);
}
