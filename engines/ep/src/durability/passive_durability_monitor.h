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

#include <queue>

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

    void notifyLocalPersistence() override;

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
        /**
         * @param pdm The owning PassiveDurabilityMonitor
         */
        State(const PassiveDurabilityMonitor& pdm) : pdm(pdm) {
        }

        /**
         * Returns the next position for a given Container::iterator.
         *
         * @param it The iterator
         * @return the next position in Container
         */
        Container::iterator getIteratorNext(const Container::iterator& it);

        /**
         * Logically 'moves' forward the High Prepared Seqno to the last
         * locally-satisfied Prepare. In other terms, the function moves the HPS
         * to before the current durability-fence.
         *
         * Details.
         *
         * In terms of Durability Requirements, Prepares at Replica can be
         * locally-satisfied:
         * (1) as soon as the they are queued into the PDM, if Level Majority or
         *     MajorityAndPersistOnMaster
         * (2) when they are persisted, if Level PersistToMajority
         *
         * We call the first non-satisfied PersistToMajority Prepare the
         * "durability-fence". All Prepares /before/ the durability-fence are
         * locally-satisfied and can be ack'ed back to the Active.
         *
         * This functions's internal logic performs (2) first by moving the HPS
         * up to the latest persisted Prepare (i.e., the durability-fence) and
         * then (1) by moving to the HPS to the last Prepare /before/ the new
         * durability-fence (note that after step (2) the durability-fence has
         * implicitly moved as well).
         */
        void updateHighPreparedSeqno();

        /// The container of pending Prepares.
        Container trackedWrites;

        // The seqno of the last Prepare satisfied locally. I.e.:
        //     - the Prepare has been queued into the PDM, if Level Majority
        //         or MajorityAndPersistToMaster
        //     - the Prepare has been persisted locally, if Level
        //         PersistToMajority
        Position highPreparedSeqno;

        const PassiveDurabilityMonitor& pdm;
    };

    // The VBucket owning this DurabilityMonitor instance
    VBucket& vb;

    folly::Synchronized<State> state;
};
