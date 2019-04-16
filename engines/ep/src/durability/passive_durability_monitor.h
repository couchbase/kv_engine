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

#include "durability_monitor.h"
#include "item.h"

#include <folly/Synchronized.h>

class VBucket;

/*
 * The DurabilityMonitor for Replica VBuckets.
 *
 * The PassiveDurabilityMonitor (PDM) is responsible for ack'ing received
 * Prepares back to the Active. The logic in the PDM ensures that Prepares are
 * ack'ed in seqno-order, which is fundamental for achieving:
 * - In-Order Commit at Active
 * - Consistency at failure scenarios
 */
class PassiveDurabilityMonitor : public DurabilityMonitor {
public:
    PassiveDurabilityMonitor(VBucket& vb);
    ~PassiveDurabilityMonitor();

    void addStats(const AddStatFn& addStat, const void* cookie) const override;

    int64_t getHighPreparedSeqno() const override;

    /**
     * Add a pending Prepare for tracking into the PDM.
     *
     * @param item the queued_item
     */
    void addSyncWrite(queued_item item);

    size_t getNumTracked() const override;

protected:
    void toOStream(std::ostream& os) const override;

    /*
     * This class embeds the state of a PDM. It has been designed for being
     * wrapped by a folly::Synchronized<T>, which manages the read/write
     * concurrent access to the T instance.
     * Note: all members are public as accessed directly only by PDM, this is
     * a protected struct. Avoiding direct access by PDM would require
     * re-implementing most of the PDM functions into PDM::State and exposing
     * them on the PDM::State public interface.
     */
    struct State {
        /// The container of pending Prepares.
        Container trackedWrites;
    };

    // The VBucket owning this DurabilityMonitor instance
    VBucket& vb;

    folly::Synchronized<State> state;
};
