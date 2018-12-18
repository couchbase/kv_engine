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
#include "stored-value.h"

#include <unordered_map>

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
            memoryIterators[node] = it;
        }
    }

    // Index of replica iterators. The key is the replica id.
    // Each iterator points to the last SyncWrite ack'ed by the replica.
    // Note that the SyncWrite at iterator embeds the in-memory state of the
    // replica (via the SyncWrite seqno).
    // So, the DurabilityMonitor internal logic ensures that each iterator in
    // this map have only 2 possible states:
    // - points to Container::end(), if Container is empty
    // - points to an element of Container otherwise
    // I.e., an iterator is never singular.
    // That implies also that in the current DurabilityMonitor implementation
    // Container is empty only before the first SyncWrite is added for tracking.
    std::unordered_map<std::string, Container::iterator> memoryIterators;

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
    // @todo: The scope can be probably shorten. Deferring to follow-up patches
    //     as I'm amending this function considerably.
    std::lock_guard<std::mutex> lg(state.m);

    // Note that in the current implementation of DurabilitMonitot Container
    // can be empty only before the first SyncWrite is added for tracking.
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

        // Update replica iterator
        advanceReplicaMemoryIterator(lg, replica);

        // Note: In this first implementation (1 replica) the Durability
        // Requirement for the pending SyncWrite is implicitly verified
        // at this point.

        // Commit the verified SyncWrite
        commit(lg);
    }

    return ENGINE_SUCCESS;
}

size_t DurabilityMonitor::getNumTracked(
        const std::lock_guard<std::mutex>& lg) const {
    return state.trackedWrites.size();
}

const DurabilityMonitor::Container::iterator&
DurabilityMonitor::getReplicaMemoryIterator(
        const std::lock_guard<std::mutex>& lg,
        const std::string& replica) const {
    if (!state.firstChain) {
        throw std::logic_error(
                "DurabilityMonitor::getReplicaMemoryIterator: no chain "
                "registered");
    }
    const auto entry = state.firstChain->memoryIterators.find(replica);
    if (entry == state.firstChain->memoryIterators.end()) {
        throw std::invalid_argument(
                "DurabilityMonitor::getReplicaEntry: replica " + replica +
                " not found in chain");
    }
    return entry->second;
}

DurabilityMonitor::Container::iterator DurabilityMonitor::getReplicaMemoryNext(
        const std::lock_guard<std::mutex>& lg, const std::string& replica) {
    const auto& it = getReplicaMemoryIterator(lg, replica);
    // A replica iterator represent the durability state of a replica as seen
    // from the active and it is never singular:
    // 1) points to container.end()
    //     a) if container is empty, which cannot be the case here
    //     b) before the active has received the first ACK
    // 2) points to an element of container otherwise
    //
    // In the 1b) case the next iterator position is Container::begin.
    return (it == state.trackedWrites.end()) ? state.trackedWrites.begin()
                                             : std::next(it);
}

void DurabilityMonitor::advanceReplicaMemoryIterator(
        const std::lock_guard<std::mutex>& lg, const std::string& replica) {
    auto& it = const_cast<DurabilityMonitor::Container::iterator&>(
            getReplicaMemoryIterator(lg, replica));
    it++;
}

int64_t DurabilityMonitor::getReplicaMemorySeqno(
        const std::lock_guard<std::mutex>& lg,
        const std::string& replica) const {
    const auto& it = getReplicaMemoryIterator(lg, replica);
    if (it == state.trackedWrites.end()) {
        return 0;
    }
    return it->getBySeqno();
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

void DurabilityMonitor::commit(const std::lock_guard<std::mutex>& lg) {
    // @todo: do commit.
    // Here we will:
    // 1) update the SyncWritePrepare to SyncWriteCommit in the HT
    // 2) enqueue a SyncWriteCommit item into the CM
    // 3) send a response with Success back to the client
}
