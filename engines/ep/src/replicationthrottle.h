/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2010-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#pragma once

#include <platform/socket.h>
#include <relaxed_atomic.h>

class EPStats;
class Configuration;

/**
 * ReplicationThrottle interface.
 *
 * Monitors various internal state to report whether we should
 * throttle incoming DCP items.
 */
class ReplicationThrottle {
public:
    /* Indicates the status of the replication throttle */
    enum class Status { Process, Pause, Disconnect };

    /**
     * @ return status of the replication throttle
     */
    virtual Status getStatus() const = 0;

    /**
     * Returns if we should disconnect replication connection upon hitting
     * ENOMEM during replication
     *
     * @return boolean indicating whether we should disconnect
     */
    virtual bool doDisconnectOnNoMem() const = 0;

    /**
     * Set the percentage of total items in write queue at which we throttle
     * replication input.
     */
    virtual void setCapPercent(size_t perc) = 0;

    /// Set the max size of write queue to throttle incoming replication input.
    virtual void setQueueCap(ssize_t cap) = 0;

    /**
     * Set the writeQueueCap to the specified number of items, adjusting
     * as appropriate based on CapPercent / QueueCap values.
     */
    virtual void adjustWriteQueueCap(size_t totalItems) = 0;

    virtual ~ReplicationThrottle() = default;
};

/**
 * Main concrete implementation, used for EP bucket (and inherited from for
 * Ephemeral buckets).
 */
class ReplicationThrottleEP : public ReplicationThrottle {
public:
    ReplicationThrottleEP(const Configuration& config, EPStats& s);

    Status getStatus() const override;

    bool doDisconnectOnNoMem() const override {
        /* For EP buckets we do not want to disconnect */
        return false;
    }

    void setCapPercent(size_t perc) override {
        capPercent = perc;
    }
    void setQueueCap(ssize_t cap) override {
        queueCap = cap;
    }

    void adjustWriteQueueCap(size_t totalItems) override;

private:
    bool persistenceQueueSmallEnough() const;
    bool hasSomeMemory() const;

    cb::RelaxedAtomic<ssize_t> queueCap;
    cb::RelaxedAtomic<size_t> capPercent;
    EPStats &stats;
};

/**
 * Sub class of ReplicationThrottle which decides what should be done to
 * throttle DCP replication, that is Pause or Disconnect, in Ephemeral buckets
 */
class ReplicationThrottleEphe : public ReplicationThrottleEP {
public:
    ReplicationThrottleEphe(const Configuration& config, EPStats& s);

    Status getStatus() const override;

    bool doDisconnectOnNoMem() const override;

private:
    const Configuration& config;
};
