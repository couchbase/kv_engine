/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2016 Couchbase, Inc
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

#include "config.h"

#include <atomic>
#include <climits>
#include <list>
#include <string>

#include "ep_engine.h"
#include "locks.h"
#include "syncobject.h"
#include "atomicqueue.h"
#include "connmap.h"
#include "dcp/consumer.h"
#include "dcp/producer.h"

class DcpConnMap : public ConnMap {

public:

    DcpConnMap(EventuallyPersistentEngine &engine);

    /**
     * Find or build a dcp connection for the given cookie and with
     * the given name.
     */
    DcpProducer *newProducer(const void* cookie, const std::string &name,
                             bool notifyOnly);


    /**
     * Create a new consumer and add it in the list of TapConnections
     * @param e the engine
     * @param c the cookie representing the client
     * @return Pointer to the new dcp connection
     */
    DcpConsumer *newConsumer(const void* cookie, const std::string &name);

    void notifyVBConnections(uint16_t vbid, uint64_t bySeqno);

    void notifyBackfillManagerTasks();

    void removeVBConnections(connection_t &conn);

    /**
     * Close outbound (active) streams for a vbucket whenever a state
     * change is detected. In case of failovers, close inbound (passive)
     * streams as well.
     *
     * @param vbucket the vbucket id
     * @param state the new state of the vbucket
     * @closeInboundStreams bool flag indicating failover
     */
    void vbucketStateChanged(uint16_t vbucket, vbucket_state_t state,
                             bool closeInboundStreams = true);

    void shutdownAllConnections();

    /**
     * Handles the slow stream with the specified name.
     * Returns true if the stream dropped its cursors on the
     * checkpoint.
     */
    bool handleSlowStream(uint16_t vbid, const std::string &name);

    void disconnect(const void *cookie);

    void manageConnections();

    bool canAddBackfillToActiveQ();

    void decrNumActiveSnoozingBackfills();

    void updateMaxActiveSnoozingBackfills(size_t maxDataSize);

    uint16_t getNumActiveSnoozingBackfills () {
        SpinLockHolder lh(&numBackfillsLock);
        return numActiveSnoozingBackfills;
    }

    uint16_t getMaxActiveSnoozingBackfills () {
        SpinLockHolder lh(&numBackfillsLock);
        return maxActiveSnoozingBackfills;
    }

    ENGINE_ERROR_CODE addPassiveStream(ConnHandler& conn, uint32_t opaque,
                                       uint16_t vbucket, uint32_t flags);

    /* Use this only for any quick direct stats from DcpConnMap. To collect
       individual conn stats from conn lists please use ConnStatBuilder */
    void addStats(ADD_STAT add_stat, const void *c);

    /* Updates the minimum compression ratio to be achieved for docs by
     * all the producers, which will be in effect if the producer side
     * value compression is enabled */
    void updateMinCompressionRatioForProducers(float value);

    float getMinCompressionRatio();

protected:
    /*
     * deadConnections is protected (as opposed to private) because
     * of the module test ep-engine_dead_connections_test
     */
    std::list<connection_t> deadConnections;

private:

    bool isPassiveStreamConnected_UNLOCKED(uint16_t vbucket);

    void disconnect_UNLOCKED(const void *cookie);

    void closeAllStreams_UNLOCKED();

    void cancelAllTasks_UNLOCKED();

    SpinLock numBackfillsLock;
    /* Db file memory */
    static const uint32_t dbFileMem;
    uint16_t numActiveSnoozingBackfills;
    uint16_t maxActiveSnoozingBackfills;
    /* Max num of backfills we want to have irrespective of memory */
    static const uint16_t numBackfillsThreshold;
    /* Max percentage of memory we want backfills to occupy */
    static const uint8_t numBackfillsMemThreshold;

    std::atomic<float> minCompressionRatioForProducer;

    /* Total memory used by all DCP consumer buffers */
    std::atomic<size_t> aggrDcpConsumerBufferSize;

};
