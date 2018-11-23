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

#include "ep_types.h"

#include "memcached/durability_spec.h"
#include "memcached/engine_error.h"

#include <list>
#include <mutex>

class VBucket;
class StoredValue;

/**
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
     * Start tracking a new SyncWrite.
     * Expected to be called by VBucket::add/update/delete after a new SyncWrite
     * has been inserted into the HashTable and enqueued into the
     * CheckpointManager.
     *
     * @param sv the StoredValue
     * @param durReq the Durability Requirement
     * @return ENGINE_SUCCESS if the operation succeeds, an error code otherwise
     */
    ENGINE_ERROR_CODE addSyncWrite(const StoredValue& sv,
                                   cb::durability::Requirements durReq);

    /**
     * Expected to be called by memcached at receiving a DCP_SEQNO_ACK packet.
     *
     * @throw std::logic_error if the received seqno is unexpected
     */
    ENGINE_ERROR_CODE seqnoAckReceived(int64_t memorySeqno);

protected:
    /**
     * @return the number of pending SyncWrite(s) currently tracked
     */
    size_t getNumTracked() const;

    /**
     * @return the memory-seqno for the replica
     *
     * @todo: Expand for supporting multiple-replicas and disk-seqno
     */
    int64_t getReplicaSeqno() const;

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

    class SyncWrite;
    using Container = std::list<SyncWrite>;

    // The list of tracked SyncWrites
    struct {
        mutable std::mutex m;
        Container container;
    } trackedWrites;
};
