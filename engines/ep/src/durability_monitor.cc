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

#include "bucket_logger.h"
#include "item.h"
#include "monotonic.h"
#include "statwriter.h"
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
 * - lastWriteSeqno: Stores always the seqno of the last SyncWrite
 *         acknowledged by the tracked replica, even when Position::it points to
 *         Container::end. Used for validation at seqno-ack received and stats.
 *
 * - lastAckSeqno: Stores always the last seqno acknowledged by the tracked
 *         replica. Used for validation at seqno-ack received and stats.
 */
struct DurabilityMonitor::Position {
    Position(const Container::iterator& it) : it(it) {
    }
    Container::iterator it;
    WeaklyMonotonic<int64_t, ThrowExceptionPolicy> lastWriteSeqno{0};
    WeaklyMonotonic<int64_t, ThrowExceptionPolicy> lastAckSeqno{0};
};

struct DurabilityMonitor::NodePosition {
    Position memory;
    Position disk;
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
 * i.e. a list of active/replica nodes where the VBucket resides.
 */
struct DurabilityMonitor::ReplicationChain {
    /**
     * @param nodes The list of active/replica nodes in the ns_server format:
     *     {active, replica1, replica2, replica3}
     */
    ReplicationChain(const std::vector<std::string>& nodes,
                     const Container::iterator& it)
        : majority(nodes.size() / 2 + 1) {
        for (const auto& node : nodes) {
            // This check ensures that there is no duplicate in the given chain
            if (!positions
                         .emplace(node,
                                  NodePosition{Position(it), Position(it)})
                         .second) {
                throw std::invalid_argument(
                        "ReplicationChain::ReplicationChain: Duplicate node: " +
                        node);
            }
        }
        Ensures(positions.size() == nodes.size());
    }

    // Index of node Positions. The key is the replica id.
    // A Position embeds the seqno-state of the tracked node.
    std::unordered_map<std::string, NodePosition> positions;

    // Majority in the arithmetic definition: num-nodes / 2 + 1
    const uint8_t majority;
};

DurabilityMonitor::DurabilityMonitor(VBucket& vb) : vb(vb) {
}

DurabilityMonitor::~DurabilityMonitor() = default;

ENGINE_ERROR_CODE DurabilityMonitor::registerReplicationChain(
        const std::vector<std::string>& nodes) {
    if (nodes.size() == 0) {
        throw std::logic_error(
                "DurabilityMonitor::registerReplicationChain: Empty chain not "
                "allowed");
    }

    if (nodes.size() == 1) {
        throw std::logic_error(
                "DurabilityMonitor::registerReplicationChain: Chain contains "
                "only the active node");
    }

    if (nodes.size() > 2) {
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

    if (durReq.getLevel() == cb::durability::Level::None) {
        throw std::invalid_argument(
                "DurabilityMonitor::addSyncWrite: Level::None");
    }

    if (durReq.getLevel() ==
                cb::durability::Level::MajorityAndPersistOnMaster ||
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
        const std::string& replica, int64_t memorySeqno, int64_t diskSeqno) {
    if (memorySeqno < diskSeqno) {
        throw std::invalid_argument(
                "DurabilityMonitor::seqnoAckReceived: memorySeqno < diskSeqno "
                "(" +
                std::to_string(memorySeqno) + " < " +
                std::to_string(diskSeqno) + ")");
    }

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
                    "SyncWrite, but replica ack'ed {memorySeqno:" +
                    std::to_string(memorySeqno) +
                    ", diskSeqno:" + std::to_string(diskSeqno));
        }

        processSeqnoAck(lg, replica, Tracking::Memory, memorySeqno, toCommit);
        // @todo: Process disk-seqno
    }

    // Commit the verified SyncWrites
    for (const auto& entry : toCommit) {
        commit(entry.getKey(), entry.getBySeqno(), entry.getCookie());
    }

    return ENGINE_SUCCESS;
}

void DurabilityMonitor::addStats(const AddStatFn& addStat,
                                 const void* cookie) const {
    std::lock_guard<std::mutex> lg(state.m);
    char buf[256];

    try {
        const auto vbid = vb.getId().get();

        checked_snprintf(buf, sizeof(buf), "vb_%d:state", vbid);
        add_casted_stat(buf, VBucket::toString(vb.getState()), addStat, cookie);

        checked_snprintf(buf, sizeof(buf), "vb_%d:num_tracked", vbid);
        add_casted_stat(buf, getNumTracked(lg), addStat, cookie);

        checked_snprintf(
                buf, sizeof(buf), "vb_%d:replication_chain_first:size", vbid);
        add_casted_stat(buf, getReplicationChainSize(lg), addStat, cookie);

        for (const auto& entry : state.firstChain->positions) {
            const auto* replica = entry.first.c_str();
            const auto& pos = entry.second;

            checked_snprintf(
                    buf,
                    sizeof(buf),
                    "vb_%d:replication_chain_first:%s:memory:last_write_seqno",
                    vbid,
                    replica);
            add_casted_stat(buf, pos.memory.lastWriteSeqno, addStat, cookie);
            checked_snprintf(
                    buf,
                    sizeof(buf),
                    "vb_%d:replication_chain_first:%s:memory:last_ack_seqno",
                    vbid,
                    replica);
            add_casted_stat(buf, pos.memory.lastAckSeqno, addStat, cookie);

            checked_snprintf(
                    buf,
                    sizeof(buf),
                    "vb_%d:replication_chain_first:%s:disk:last_write_seqno",
                    vbid,
                    replica);
            add_casted_stat(buf, pos.memory.lastWriteSeqno, addStat, cookie);
            checked_snprintf(
                    buf,
                    sizeof(buf),
                    "vb_%d:replication_chain_first:%s:disk:last_ack_seqno",
                    vbid,
                    replica);
            add_casted_stat(buf, pos.disk.lastAckSeqno, addStat, cookie);
        }

    } catch (const std::exception& e) {
        EP_LOG_WARN("DurabilityMonitor::addStats: error building stats: {}",
                    e.what());
    }
}

std::string DurabilityMonitor::to_string(Tracking tracking) const {
    auto value = std::to_string(static_cast<uint8_t>(tracking));
    switch (tracking) {
    case Tracking::Memory:
        return value + ":memory";
    case Tracking::Disk:
        return value + ":disk";
    };
    return value + ":invalid";
}

size_t DurabilityMonitor::getNumTracked(
        const std::lock_guard<std::mutex>& lg) const {
    return state.trackedWrites.size();
}

size_t DurabilityMonitor::getReplicationChainSize(
        const std::lock_guard<std::mutex>& lg) const {
    return state.firstChain->positions.size();
}

DurabilityMonitor::Container::iterator DurabilityMonitor::getNodeNext(
        const std::lock_guard<std::mutex>& lg,
        const std::string& node,
        Tracking tracking) {
    const auto& pos = state.firstChain->positions.at(node);
    const auto& it =
            (tracking == Tracking::Memory ? pos.memory.it : pos.disk.it);
    // Note: Container::end could be the new position when the pointed SyncWrite
    //     is removed from Container and the iterator repositioned.
    //     In that case next=Container::begin
    return (it == state.trackedWrites.end()) ? state.trackedWrites.begin()
                                             : std::next(it);
}

void DurabilityMonitor::advanceNodePosition(
        const std::lock_guard<std::mutex>& lg,
        const std::string& node,
        Tracking tracking) {
    const auto& pos_ = state.firstChain->positions.at(node);
    auto& pos = const_cast<Position&>(tracking == Tracking::Memory ? pos_.memory
                                                                   : pos_.disk);

    if (pos.it == state.trackedWrites.end()) {
        pos.it = state.trackedWrites.begin();
    } else {
        pos.it++;
    }

    Expects(pos.it != state.trackedWrites.end());

    // Note that Position::lastWriteSeqno is always set to the current
    // pointed SyncWrite to keep the replica seqno-state for when the pointed
    // SyncWrite is removed
    pos.lastWriteSeqno = pos.it->getBySeqno();
}

DurabilityMonitor::NodeSeqnos DurabilityMonitor::getNodeWriteSeqnos(
        const std::lock_guard<std::mutex>& lg, const std::string& node) const {
    const auto& pos = state.firstChain->positions.at(node);
    return {pos.memory.lastWriteSeqno, pos.disk.lastWriteSeqno};
}

DurabilityMonitor::NodeSeqnos DurabilityMonitor::getNodeAckSeqnos(
        const std::lock_guard<std::mutex>& lg, const std::string& node) const {
    const auto& pos = state.firstChain->positions.at(node);
    return {pos.memory.lastAckSeqno, pos.disk.lastAckSeqno};
}

DurabilityMonitor::Container DurabilityMonitor::removeSyncWrite(
        const std::lock_guard<std::mutex>& lg, const Position& pos) {
    if (pos.it == state.trackedWrites.end()) {
        throw std::logic_error(
                "DurabilityMonitor::commit: Position points to end");
    }

    // Note that Position.seqno stays set to the original value. That way we
    // keep the replica seqno-state even after the SyncWrite is removed.
    auto removeSeqno = pos.lastWriteSeqno;
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

    // Removing the element at pos.it from trackedWrites invalidates any
    // iterator that points to that element. So, we have to reposition the
    // invalidated iterators after the removal.
    // Note: the following will pick up also pos.it itself.
    // Note: O(N) with N=<number of iterators>, max(N)=12
    //     (max 2 chains, 3 replicas, 2 iterators per replica)
    for (const auto& entry : state.firstChain->positions) {
        if (entry.second.memory.lastWriteSeqno == removeSeqno) {
            const_cast<Position&>(entry.second.memory).it = prev;
        }
        if (entry.second.disk.lastWriteSeqno == removeSeqno) {
            const_cast<Position&>(entry.second.disk).it = prev;
        }
    }

    Ensures(pos_.lastWriteSeqno == removeSeqno);

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

void DurabilityMonitor::processSeqnoAck(const std::lock_guard<std::mutex>& lg,
                                        const std::string& node,
                                        Tracking tracking,
                                        int64_t ackSeqno,
                                        Container& toCommit) {
    if (tracking == Tracking::Disk) {
        throw std::invalid_argument(
                "DurabilityMonitor::processSeqnoAck: Tracking::Disk not "
                "supported");
    }

    // Note: process up to the ack'ed seqno
    DurabilityMonitor::Container::iterator next;
    while ((next = getNodeNext(lg, node, tracking)) !=
                   state.trackedWrites.end() &&
           next->getBySeqno() <= ackSeqno) {
        // Update replica tracking
        advanceNodePosition(lg, node, tracking);

        // Note
        // Supporting only:
        // - 1 replica
        // - Level {Majority}
        // So the Durability Requirements for a tracked SyncWrite are verified
        // as soon as the tracking iterator points to it.

        const auto& pos_ = state.firstChain->positions.at(node);
        const auto& pos =
                (tracking == Tracking::Memory ? pos_.memory : pos_.disk);
        auto removed = removeSyncWrite(lg, pos);
        toCommit.splice(toCommit.end(), removed);
    }

    // We keep track of the actual ack'ed seqno
    const auto& pos_ = state.firstChain->positions.at(node);
    auto& pos = const_cast<Position&>(tracking == Tracking::Memory ? pos_.memory
                                                                   : pos_.disk);

    // Note: using WeaklyMonotonic, as receiving the same seqno multiple times
    // for the same node is ok. That just means that the node has not advanced
    // any of memory/disk seqnos.
    // E.g., imagine the following DCP_SEQNO_ACK sequence:
    //
    // {mem:1, disk:0} -> {mem:2, disk:0}
    //
    // That is legal, and it means that the node has enqueued seqnos {1, 2}
    // but not persisted anything yet.
    //
    // @todo: By doing this I don't catch the case where the replica has ack'ed
    //     both the same mem/disk seqnos twice (which shouldn't happen).
    //     It would be good to catch that, useful for replica logic-check.
    pos.lastAckSeqno = ackSeqno;
}
