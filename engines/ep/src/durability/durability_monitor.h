/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2019 Couchbase, Inc.
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

#include "memcached/engine_common.h"
#include "monotonic.h"
#include <list>

/*
 * Base (abstract) class for DurabilityMonitor.
 */
class DurabilityMonitor {
public:
    virtual ~DurabilityMonitor() = default;

    /**
     * Output DurabiltyMonitor stats.
     *
     * @param addStat the callback to memcached
     * @param cookie
     */
    virtual void addStats(const AddStatFn& addStat,
                          const void* cookie) const = 0;

    /// @return the high_prepared_seqno.
    virtual int64_t getHighPreparedSeqno() const = 0;

    /**
     * @return the number of pending SyncWrite(s) currently tracked
     */
    virtual size_t getNumTracked() const = 0;

    /**
     * Inform the DurabilityMonitor that the Flusher has run.
     * Expected to be called by the Flusher after a flush-batch (that contains
     * pending Prepares) has been committed to the storage.
     */
    virtual void notifyLocalPersistence() = 0;

    enum class ReplicationChainName {
        First = 1,
        Second = 2,
    };

protected:
    class SyncWrite;
    struct ReplicationChain;
    struct Position;

    using Container = std::list<SyncWrite>;

    virtual void toOStream(std::ostream& os) const = 0;

    friend std::ostream& operator<<(std::ostream& os,
                                    const DurabilityMonitor& dm);

    friend std::ostream& operator<<(std::ostream&, const SyncWrite&);
};

/**
 * Represents the tracked state of a node in topology.
 * Note that the lifetime of a Position is determined by the logic in
 * DurabilityMonitor.
 *
 * - it: Iterator that points to a position in the Container of tracked
 *         SyncWrites. This is an optimization: logically it points always
 * to the last SyncWrite acknowledged by the tracked node, so that we can
 * avoid any O(N) scan when updating the node state at seqno-ack received.
 * It may point to Container::end (e.g, when the pointed SyncWrite is the
 * last element in Container and it is removed).
 *
 * - lastWriteSeqno: Stores always the seqno of the last SyncWrite
 *         acknowledged by the tracked node, even when Position::it points
 * to Container::end. Used for validation at seqno-ack received and stats.
 *
 * - lastAckSeqno: Stores always the last seqno acknowledged by the tracked
 *         node. Used for validation at seqno-ack received and stats.
 */
struct DurabilityMonitor::Position {
    Position() = default;
    Position(const Container::iterator& it) : it(it) {
    }
    Container::iterator it;
    // @todo: Consider using (strictly) Monotonic here. Weakly monotonic was
    // necessary when we tracked both memory and disk seqnos.
    // Now a Replica is not supposed to ack the same seqno twice.
    WeaklyMonotonic<int64_t, ThrowExceptionPolicy> lastWriteSeqno{0};
    WeaklyMonotonic<int64_t, ThrowExceptionPolicy> lastAckSeqno{0};
};
