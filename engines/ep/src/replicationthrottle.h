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

#ifndef SRC_REPLICATIONTHROTTLE_H_
#define SRC_REPLICATIONTHROTTLE_H_ 1

#include "config.h"

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

    /**
     * @ return status of the replication throttle
     */
    virtual ReplicationThrottle::Status getStatus() const;

    void setCapPercent(size_t perc) { capPercent = perc; }
    void setQueueCap(ssize_t cap) { queueCap = cap; }

    void adjustWriteQueueCap(size_t totalItems);

private:
    bool persistenceQueueSmallEnough() const;
    bool hasSomeMemory() const;

    Couchbase::RelaxedAtomic<ssize_t> queueCap;
    Couchbase::RelaxedAtomic<size_t> capPercent;
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

private:
    const Configuration& config;
};

#endif  // SRC_REPLICATIONTHROTTLE_H_
