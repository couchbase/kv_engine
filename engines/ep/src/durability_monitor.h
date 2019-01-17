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
#include "memcached/engine_common.h"
#include "memcached/engine_error.h"

#include <list>
#include <mutex>

class StoredDocKey;
class StoredValue;
class VBucket;

/*
 * The DurabilityMonitor (DM) drives the finalization (commit/abort) of a
 * SyncWrite request.
 *
 * To do that, the DM tracks the pending SyncWrites and the replica
 * acknowledgements to verify if the Durability Requirement is satisfied for
 * the tracked mutations.
 *
 * DM internals (describing by example).
 *
 * num_replicas: 1
 * durability_level: Majority (tracking only in-memory seqno)
 * M: Memory-seqno position of tracked replica
 *
 * Tracked:        rEnd        2        6        7        8        15
 *                 ^
 *                 M
 *
 * At seqno-ack received, M is moved to the latest seqno <= seqno-ack.
 * Currently (1 replica, in-memory seqno, 1 replication-chain only), at each
 * move the Durability Requirements of the SyncWrite at the new position are
 * implicitly verified, so the SyncWrite at the new position is committed and
 * removed.
 * E.g., at receiving a seqno-12 ack the new internal state will be:
 *
 * Tracked:        rEnd        15
 *                 ^
 *                 M
 *
 * Note that M maintains internally the tracked replica state (last ack'ed
 * seqno and last ack'ed SyncWrite seqno) even after the pointed SyncWrite is
 * removed.
 */
class DurabilityMonitor {
public:
    struct ReplicaSeqnos {
        int64_t memory;
        int64_t disk;
    };

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
     * @param cookie Optional client cookie which will be notified the SyncWrite
     *        completes.
     * @param item the queued_item
     * @return ENGINE_SUCCESS if the operation succeeds, an error code otherwise
     */
    ENGINE_ERROR_CODE addSyncWrite(const void* cookie, queued_item item);

    /**
     * Expected to be called by memcached at receiving a DCP_SEQNO_ACK packet.
     *
     * @param replica The replica that sent the ACK
     * @param memorySeqno The ack'ed memory-seqno
     * @param diskSeqno The ack'ed disk-seqno
     * @return ENGINE_SUCCESS if the operation succeeds, an error code otherwise
     * @throw std::logic_error if the received seqno is unexpected
     *
     * @todo: Expand for  supporting a full {memorySeqno, diskSeqno} ACK.
     */
    ENGINE_ERROR_CODE seqnoAckReceived(const std::string& replica,
                                       int64_t memorySeqno,
                                       int64_t diskSeqno);

    /**
     * Output DurabiltyMonitor stats.
     *
     * @param addStat the callback to memcached
     * @param cookie
     */
    void addStats(ADD_STAT addStat, const void* cookie) const;

protected:
    class SyncWrite;
    struct ReplicationChain;
    struct Position;
    struct ReplicaPosition;

    using Container = std::list<SyncWrite>;

    enum class Tracking : uint8_t { Memory, Disk };

    std::string to_string(Tracking tracking) const;

    /**
     * @param lg the object lock
     * @return the number of pending SyncWrite(s) currently tracked
     */
    size_t getNumTracked(const std::lock_guard<std::mutex>& lg) const;

    /**
     * @param lg the object lock
     * @return the size of the replication chain
     */
    size_t getReplicationChainSize(const std::lock_guard<std::mutex>& lg) const;

    /**
     * Returns the next position for a replica iterator.
     *
     * @param lg the object lock
     * @param replica
     * @param tracking Memory or Disk?
     * @return the iterator to the next position for the given replica
     */
    Container::iterator getReplicaNext(const std::lock_guard<std::mutex>& lg,
                                       const std::string& replica,
                                       Tracking tracking);

    /**
     * Advance a replica tracking to the next Position in the tracked Container.
     * Note that a Position tracks a replica in terms of both:
     * - iterator to a SyncWrite in the tracked Container
     * - seqno of the last SyncWrite ack'ed by the replica
     * This function advances both iterator and seqno.
     *
     * @param lg the object lock
     * @param replica
     * @param tracking Memory or Disk?
     */
    void advanceReplicaPosition(const std::lock_guard<std::mutex>& lg,
                                const std::string& replica,
                                Tracking tracking);

    /**
     * Returns the seqnos of the SyncWrites currently pointed by the internal
     * memory/disk tracking for Replica.
     * E.g., if we have a tracked SyncWrite list like {s:1, s:2} and we receive
     * a SeqnoAck{mem:2, disk:1}, then the internal memory/disk tracking
     * will be {mem:2, disk:1}, which is what this function returns.
     * Note that this may differ from Replica AckSeqno. Using the same example,
     * if we receive a SeqnoAck{mem:100, disk:100} then the internal tracking
     * will still point to {mem:2, disk:1}, which is what this function will
     * return again.
     *
     * @param lg the object lock
     * @param replica
     * @return the {memory, disk} seqnos of the tracked writes for replica
     */
    ReplicaSeqnos getReplicaWriteSeqnos(const std::lock_guard<std::mutex>& lg,
                                        const std::string& replica) const;

    /**
     * Returns the last {memSeqno, diskSeqno} ack'ed by Replica.
     * Note that this may differ from Replica WriteSeqno.
     *
     * @param lg the object lock
     * @param replica
     * @return the last {memory, disk} seqnos ack'ed by replica
     */
    ReplicaSeqnos getReplicaAckSeqnos(const std::lock_guard<std::mutex>& lg,
                                      const std::string& replica) const;

    /**
     * Remove the given SyncWrte from tracking.
     *
     * @param lg the object lock
     * @param pos the Position of the SyncWrite to be removed
     * @return single-element list of the removed SyncWrite.
     */
    Container removeSyncWrite(const std::lock_guard<std::mutex>& lg,
                              const Position& pos);

    /**
     * Commit the given SyncWrite.
     *
     * @param key the key of the SyncWrite to be committed
     * @param seqno the seqno of the SyncWrite to be committed
     * @param cookie The cookie of the connection to notify.
     */
    void commit(const StoredDocKey& key, int64_t seqno, const void* cookie);

    /**
     * Updates a replica memory/disk tracking as driven by the new ack-seqno.
     *
     * @param lg The object lock
     * @param replica The replica that ack'ed the given seqno
     * @param tracking Memory or Disk?
     * @param ackSeqno
     * @param [out] toCommit
     */
    void processSeqnoAck(const std::lock_guard<std::mutex>& lg,
                         const std::string& replica,
                         Tracking tracking,
                         int64_t ackSeqno,
                         Container& toCommit);

    // The VBucket owning this DurabilityMonitor instance
    VBucket& vb;

    // Represents the internal state of a DurabilityMonitor instance.
    // Any state change must happen under lock(state.m).
    struct {
        mutable std::mutex m;
        // @todo: Expand for supporting the SecondChain.
        std::unique_ptr<ReplicationChain> firstChain;
        Container trackedWrites;
    } state;
};
