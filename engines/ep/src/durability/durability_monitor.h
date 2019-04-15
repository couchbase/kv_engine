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

#include "ep_types.h"
#include "memcached/engine_common.h"

class VBucket;

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

protected:
    /**
     * @return the number of pending SyncWrite(s) currently tracked
     */
    virtual size_t getNumTracked() const = 0;

    virtual void toOStream(std::ostream& os) const = 0;

    friend std::ostream& operator<<(std::ostream& os,
                                    const DurabilityMonitor& dm);
};
