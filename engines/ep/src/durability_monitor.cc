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
#include "stored-value.h"

/*
 * Represents a tracked SyncWrite.
 *
 * Note that:
 *
 * 1) We keep a reference of the pending SyncWrite StoredValue sitting in the
 *     HashTable.
 *
 * 2) So, we don't need to keep any pointer/reference
 *     to the Prepare queued_item sitting into the CheckpointManager,
 *     as the StoredValue contains all the data we need for enqueueing the
 *     Commit queued_item into the CheckpointManager when the Durability
 *     Requirement is met.
 *
 * @todo: Considering pros/cons of this approach versus storing a queued_item
 *     (ie, ref-counted object) from the CheckpointManager, maybe changing
 */
class DurabilityMonitor::SyncWrite {
public:
    SyncWrite(const StoredValue& sv, cb::durability::Requirements durReq)
        : sv(sv), durReq(durReq) {
    }

    const StoredValue& getStoredValue() const {
        return sv;
    }

    int64_t getBySeqno() const {
        return sv.getBySeqno();
    }

private:
    const StoredValue& sv;
    const cb::durability::Requirements durReq;
};

/*
 * Represents a VBucket Replication Chain in the ns_server meaning,
 * i.e. a list of replica nodes where the VBucket replicas reside.
 */
struct DurabilityMonitor::ReplicationChain {
    ReplicationChain(const Container::iterator& it) : replicaCursor(it) {
    }

    // @todo: Extend to multiple replicaTo/persistTo cursors.
    // 1 replica, 1 replicateTo cursor for now.
    // Points to the last SyncWrite ack'ed by the replica.
    // Note that the SyncWrite at cursor embeds the state of the replica
    // (via the SyncWrite bySeqno).
    Container::iterator replicaCursor;
};

DurabilityMonitor::DurabilityMonitor(VBucket& vb) : vb(vb) {
    // Statically create a single RC.
    // @todo: Expand for creating multiple RCs dynamically.
    chain = std::make_unique<ReplicationChain>(trackedWrites.container.begin());
}

DurabilityMonitor::~DurabilityMonitor() = default;

ENGINE_ERROR_CODE DurabilityMonitor::addSyncWrite(
        const StoredValue& sv, cb::durability::Requirements durReq) {
    if (durReq.getLevel() != cb::durability::Level::Majority ||
        durReq.getTimeout() != 0) {
        return ENGINE_ENOTSUP;
    }
    std::lock_guard<std::mutex> lg(trackedWrites.m);
    trackedWrites.container.push_back(SyncWrite(sv, durReq));
    return ENGINE_SUCCESS;
}

ENGINE_ERROR_CODE DurabilityMonitor::seqnoAckReceived(int64_t memorySeqno) {
    // @todo: The scope can be probably shorten. Deferring to follow-up patches
    //     as I'm amending this function considerably.
    std::lock_guard<std::mutex> lg(trackedWrites.m);

    Container::iterator next =
            (chain->replicaCursor == trackedWrites.container.end())
                    ? trackedWrites.container.begin()
                    : std::next(chain->replicaCursor);

    if (next == trackedWrites.container.end()) {
        throw std::logic_error(
                "DurabilityManager::seqnoAckReceived: No pending SyncWrite, "
                "but replica ack'ed memorySeqno:" +
                std::to_string(memorySeqno));
    }

    int64_t pendingSeqno = (*next).getBySeqno();

    if (memorySeqno < pendingSeqno) {
        throw std::logic_error(
                "DurabilityManager::seqnoAckReceived: Ack'ed seqno is behind "
                "pending seqno {ack'ed: " +
                std::to_string(memorySeqno) +
                ", pending:" + std::to_string(pendingSeqno) + "}");
    }
    // Note: not supporting any Replica ACK optimization yet
    if (memorySeqno > pendingSeqno) {
        return ENGINE_ENOTSUP;
    }

    // Update replica state
    chain->replicaCursor = next;

    // Note: if we reach this point it is guaranteed that
    //     pendingSeqno==ACKseqno
    // So, in this first implementation the Durability Requirement for the
    // pending SyncWrite is implicitly satisfied
    // @todo: checkSatisfiedDurabilityRequirement()

    // Commit the ack'ed SyncWrite
    commit(lg);

    return ENGINE_SUCCESS;
}

size_t DurabilityMonitor::getNumTracked() const {
    std::lock_guard<std::mutex> lg(trackedWrites.m);
    return trackedWrites.container.size();
}

int64_t DurabilityMonitor::getReplicaSeqno() const {
    std::lock_guard<std::mutex> lg(trackedWrites.m);
    if (chain->replicaCursor == trackedWrites.container.end()) {
        return 0;
    }
    return (*chain->replicaCursor).getBySeqno();
}

void DurabilityMonitor::commit(const std::lock_guard<std::mutex>& lg) {
    // @todo: do commit.
    // Here we will:
    // 1) update the SyncWritePrepare to SyncWriteCommit in the HT
    // 2) enqueue a SyncWriteCommit item into the CM
    // 3) send a response with Success back to the client
}
