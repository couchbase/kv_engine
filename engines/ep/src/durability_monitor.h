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
#pragma once

#include "ep_types.h"

#include "memcached/durability_spec.h"
#include "memcached/engine_error.h"

#include <list>
#include <mutex>

class StoredValue;
class VBucket;

/*
 * The DurabilityMonitor (DM) drives the finalization (commit/abort) of a
 * SyncWrite request.
 *
 * To do that, the DM tracks the pending SyncWrites and the replica
 * acknowledgements to verify if the Durability Requirement is satisfied for
 * the tracked mutations.
 */
class DurabilityMonitor {
public:
    // Note: constructor and destructor implementation in the .cc file to allow
    // the forward declaration of ReplicationChain in the header
    DurabilityMonitor(VBucket& vb);
    ~DurabilityMonitor();

    /**
     * Track a new ReplicationChain.
     *
     * @param replicaUUIDs the set of replicas representing the chain
     * @return ENGINE_SUCCESS if the operation succeeds, an error code otherwise
     */
    ENGINE_ERROR_CODE registerReplicationChain(
            const std::vector<std::string>& replicaUUIDs);

    /**
     * Start tracking a new SyncWrite.
     * Expected to be called by VBucket::add/update/delete after a new SyncWrite
     * has been inserted into the HashTable and enqueued into the
     * CheckpointManager.
     *
     * @param item the queued_item
     * @return ENGINE_SUCCESS if the operation succeeds, an error code otherwise
     */
    ENGINE_ERROR_CODE addSyncWrite(queued_item item);

    /**
     * Expected to be called by memcached at receiving a DCP_SEQNO_ACK packet.
     *
     * @param replicaUUID uuid of the replica that sent the ACK
     * @param memorySeqno the ack'ed memory-seqno
     * @return ENGINE_SUCCESS if the operation succeeds, an error code otherwise
     * @throw std::logic_error if the received seqno is unexpected
     *
     * @todo: Expand for  supporting a full {memorySeqno, diskSeqno} ACK.
     */
    ENGINE_ERROR_CODE seqnoAckReceived(const std::string& replicaUUID,
                                       int64_t memorySeqno);

protected:
    class SyncWrite;
    using Container = std::list<SyncWrite>;

    /**
     * @return the number of pending SyncWrite(s) currently tracked
     */
    size_t getNumTracked() const;

    /**
     * Returns a replica memory iterator.
     *
     * @param replicaUUID
     * @return the iterator to the memory seqno of the given replica
     * @throw std::invalid_argument if replicaUUID is invalid
     */
    const Container::iterator& getReplicaMemoryIterator(
            const std::string& replicaUUID) const;

    /**
     * Returns the next position for a replica memory iterator.
     *
     * @param replicaUUID
     * @return the iterator to the next position for the given replica
     */
    Container::iterator getReplicaMemoryNext(const std::string& replicaUUID);

    /*
     * Advance a replica iterator
     *
     * @param replicaUUID
     * @param n number of positions it should be advanced
     * @throw std::invalid_argument if replicaUUID is invalid
     */
    void advanceReplicaMemoryIterator(const std::string& replicaUUID, size_t n);

    /**
     * Returns the memory-seqno for the replica as seen from the active.
     *
     * @param replicaUUID
     * @return the memory-seqno for the replica
     *
     * @todo: Expand for supporting and disk-seqno
     */
    int64_t getReplicaMemorySeqno(const std::string& replicaUUID) const;

    /**
     * Called internally for every SyncWrite for which the Durability
     * Requirement has been met.
     * Probably called (indirectly?) by seqnoAckReceived.
     */
    void commit(const std::lock_guard<std::mutex>& lg);

    // The VBucket owning this DurabilityMonitor instance
    VBucket& vb;

    // For now we support a single replication-chain (RC).
    //
    // @todo: Expand this in the future for supporting multiple RCs.
    //     Note that the expected number of RCs is [0, 1, 2].
    struct ReplicationChain;
    std::unique_ptr<ReplicationChain> chain;

    // The tracked SyncWrites.
    struct {
        mutable std::mutex m;
        Container container;
    } trackedWrites;
};
