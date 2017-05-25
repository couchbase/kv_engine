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
 * throttle incoming tap.
 */
class ReplicationThrottle {
public:

    ReplicationThrottle(Configuration &config, EPStats &s);

    /**
     * If true, we should process incoming tap requests.
     */
    bool shouldProcess() const;

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

#endif  // SRC_REPLICATIONTHROTTLE_H_
