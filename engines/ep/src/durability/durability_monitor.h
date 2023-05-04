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

#include <platform/memory_tracking_allocator.h>

#include <list>

class CookieIface;

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
                          const CookieIface* cookie) const = 0;

    /// @return the high_prepared_seqno.
    virtual int64_t getHighPreparedSeqno() const = 0;

    virtual int64_t getHighCompletedSeqno() const = 0;

    /**
     * Get the highest seqno of a prepare that this DM is tracking
     */
    virtual int64_t getHighestTrackedSeqno() const = 0;

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
     * @returns returns an implementation accounted value for the memory usage
     *          of all referenced Item objects (in bytes).
     */
    virtual size_t getTotalMemoryUsed() const = 0;

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

    class SyncWrite;
    /**
     * @return all of the current tracked writes
     */
    virtual std::list<SyncWrite> getTrackedWrites() const = 0;

protected:
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

/**
 * DurabilityMonitorTrackedWrites is a sub-class "wrapper" for std::list usage
 * in DurabilityMonitor sub-classes to provide memory usage tracking. It
 * requires that the Element type exposes a getSize method and for each exposed
 * modifier function, memory usage is tracked. Note that it will not account for
 * duplicated queued_item (will account twice) however it is expected that the
 * DurabilityMonitor sub-classes do not store the same
 * queued_item more than once.
 */
template <typename Element>
class DurabilityMonitorTrackedWrites
    : private std::list<Element, MemoryTrackingAllocator<Element>> {
public:
    using Base = std::list<Element, MemoryTrackingAllocator<Element>>;
    using Base::back;
    using Base::Base;
    using Base::begin;
    using Base::empty;
    using Base::end;
    using Base::front;
    using Base::get_allocator;
    using Base::size;
    using typename Base::const_iterator;
    using typename Base::iterator;

    // Require construction with an allocator
    DurabilityMonitorTrackedWrites() = delete;
    DurabilityMonitorTrackedWrites(const DurabilityMonitorTrackedWrites&) =
            delete;
    DurabilityMonitorTrackedWrites(DurabilityMonitorTrackedWrites&&) = delete;

    template <class... Args>
    void emplace_back(Args&&... args) {
        Base::emplace_back(std::forward<Args>(args)...);
        memUsed += Base::back().getSize();
    }

    iterator erase(iterator itr) {
        memUsed -= itr->getSize();
        return Base::erase(itr);
    }

    void splice(const_iterator pos,
                DurabilityMonitorTrackedWrites<Element>& other) {
        memUsed += std::exchange(other.memUsed, 0);
        Base::splice(pos, other);
    }

    void splice(const_iterator pos,
                DurabilityMonitorTrackedWrites<Element>& other,
                const_iterator it) {
        memUsed += it->getSize();
        other.memUsed -= it->getSize();
        Base::splice(pos, other, it);
    }

    // @return memory usage for all Element::getSize + the allocator's size
    size_t getTotalMemoryUsed() const {
        return memUsed + get_allocator().getBytesAllocated();
    }

    size_t getMemorySize() const {
        return memUsed;
    }

protected:

    /**
     * For all stored "Element" this tracks the sum of Element::getSize. It is
     * updated as the container is mutated by emplace/erase/splice
     */
    size_t memUsed{0};
};