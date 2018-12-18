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
     * Registers the Replication Chain.
     *
     * @param nodes the set of replicas representing the chain
     * @return ENGINE_SUCCESS if the operation succeeds, an error code otherwise
     */
    ENGINE_ERROR_CODE registerReplicationChain(
            const std::vector<std::string>& nodes);

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
     * @param replica the replica that sent the ACK
     * @param memorySeqno the ack'ed memory-seqno
     * @return ENGINE_SUCCESS if the operation succeeds, an error code otherwise
     * @throw std::logic_error if the received seqno is unexpected
     *
     * @todo: Expand for  supporting a full {memorySeqno, diskSeqno} ACK.
     */
    ENGINE_ERROR_CODE seqnoAckReceived(const std::string& replica,
                                       int64_t memorySeqno);

protected:
    class SyncWrite;
    using Container = std::list<SyncWrite>;

    /**
     * @param lg the object lock
     * @return the number of pending SyncWrite(s) currently tracked
     */
    size_t getNumTracked(const std::lock_guard<std::mutex>& lg) const;
    /**
     * Returns a replica memory iterator.
     *
     * @param lg the object lock
     * @param replica
     * @return the iterator to the memory seqno of the given replica
     * @throw std::invalid_argument if replica is not valid
     */
    const Container::iterator& getReplicaMemoryIterator(
            const std::lock_guard<std::mutex>& lg,
            const std::string& replica) const;

    /**
     * Returns the next position for a replica memory iterator.
     *
     * @param lg the object lock
     * @param replica
     * @return the iterator to the next position for the given replica
     */
    Container::iterator getReplicaMemoryNext(
            const std::lock_guard<std::mutex>& lg, const std::string& replica);

    /*
     * Advance a replica iterator
     *
     * @param lg the object lock
     * @param replica
     */
    void advanceReplicaMemoryIterator(const std::lock_guard<std::mutex>& lg,
                                      const std::string& replica);

    /**
     * Returns the memory-seqno for the replica as seen from the active.
     *
     * @param lg the object lock
     * @param replica
     * @return the memory-seqno for the replica
     *
     * @todo: Expand for supporting and disk-seqno
     */
    int64_t getReplicaMemorySeqno(const std::lock_guard<std::mutex>& lg,
                                  const std::string& replica) const;

    /*
     * @param lg the object lock
     * @param replica
     * @return true if the is a pending SyncWrite for the given replica,
     *     false otherwise
     */
    bool hasPending(const std::lock_guard<std::mutex>& lg,
                    const std::string& replica);

    /*
     * Returns the seqno of the next pending SyncWrite for the given replica.
     * The function returns 0 if replica has already acknowledged all the
     * pending seqnos.
     *
     * @param lg the object lock
     * @param replica
     * @return the pending seqno for replica, 0 if there is no pending
     */
    int64_t getReplicaPendingMemorySeqno(const std::lock_guard<std::mutex>& lg,
                                         const std::string& replica);

    /**
     * Commits all the pending SyncWrtites for which the Durability Requirement
     * has been met.
     *
     * @param lg the object lock
     */
    void commit(const std::lock_guard<std::mutex>& lg);

    // The VBucket owning this DurabilityMonitor instance
    VBucket& vb;

    struct ReplicationChain;
    // Represents the internal state of a DurabilityMonitor instance.
    // Any state change must happen under lock(state.m).
    struct {
        mutable std::mutex m;
        // @todo: Expand for supporting the SecondChain.
        std::unique_ptr<ReplicationChain> firstChain;
        Container trackedWrites;
    } state;
};
