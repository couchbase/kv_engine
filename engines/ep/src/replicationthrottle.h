/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2010 Couchbase, Inc
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

#include <relaxed_atomic.h>

#include "stats.h"

class Configuration;

/**
 * Monitors various internal state to report whether we should
 * throttle incoming tap and DCP items.
 */
class ReplicationThrottle {
public:
    /* Indicates the status of the replication throttle */
    enum class Status { Process, Pause, Disconnect };

    ReplicationThrottle(const Configuration& config, EPStats& s);

    virtual ~ReplicationThrottle() = default;

    /**
     * @ return status of the replication throttle
     */
    virtual ReplicationThrottle::Status getStatus() const;

    /**
     * Returns if we should disconnect replication connection upon hitting
     * ENOMEM during replication
     *
     * @return boolean indicating whether we should disconnect
     */
    virtual bool doDisconnectOnNoMem() const {
        /* Base class we do not want to disconnect */
        return false;
    }

    void setCapPercent(size_t perc) { capPercent = perc; }
    void setQueueCap(ssize_t cap) { queueCap = cap; }

    void adjustWriteQueueCap(size_t totalItems);

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
class ReplicationThrottleEphe : public ReplicationThrottle {
public:
    ReplicationThrottleEphe(const Configuration& config, EPStats& s);

    ReplicationThrottle::Status getStatus() const override;

    bool doDisconnectOnNoMem() const override;

private:
    const Configuration& config;
};
