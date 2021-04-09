/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2019-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#pragma once

#include "memcached/engine_common.h"
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

    virtual int64_t getHighCompletedSeqno() const = 0;

    /**
     * @return the number of pending SyncWrite(s) currently tracked
     */
    virtual size_t getNumTracked() const = 0;

    /**
     * @returns the cumulative number of SyncWrite(s) which have been
     * accepted (tracked).
     */
    virtual size_t getNumAccepted() const = 0;

    /**
     * @returns the cumulative number of SyncWrite(s) which have been
     * committed.
     */
    virtual size_t getNumCommitted() const = 0;

    /**
     * @returns the cumulative number of SyncWrite(s) which have been
     * aborted.
     */
    virtual size_t getNumAborted() const = 0;

    /**
     * Inform the DurabilityMonitor that the Flusher has run.
     * Expected to be called by the Flusher after a flush-batch (that contains
     * pending Prepares) has been committed to the storage.
     */
    virtual void notifyLocalPersistence() = 0;

    /**
     * Debug function that prints the DM state to stderr.
     */
    virtual void dump() const = 0;

    enum class ReplicationChainName {
        First = 1,
        Second = 2,
    };

protected:
    class SyncWrite;
    class ActiveSyncWrite;

    template <typename Container>
    struct Position;

    virtual void toOStream(std::ostream& os) const = 0;

    friend std::ostream& operator<<(std::ostream& os,
                                    const DurabilityMonitor& dm);

    friend std::ostream& operator<<(std::ostream&, const SyncWrite&);
    friend std::ostream& operator<<(std::ostream&, const ActiveSyncWrite&);

    /** Return a string representation of the given Position.
     *
     * @param pos
     * @param trackedWritesEnd Iterator pointing at the end() of the
     *        trackedWrites container this Position references. Used to check
     *        if Position is at end and print appropiate info.
     */
    template <typename Container>
    friend std::string to_string(
            const DurabilityMonitor::Position<Container>& pos,
            typename Container::const_iterator trackedWritesEnd);
};

std::string to_string(DurabilityMonitor::ReplicationChainName name);
