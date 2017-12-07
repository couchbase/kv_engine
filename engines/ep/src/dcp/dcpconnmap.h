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

#include "collections/filter.h"
#include "connmap.h"

#include <platform/sized_buffer.h>

#include <atomic>
#include <list>
#include <string>

class DcpProducer;
class DcpConsumer;

class DcpConnMap : public ConnMap {

public:

    DcpConnMap(EventuallyPersistentEngine &engine);

    ~DcpConnMap();

    /**
     * Find or build a dcp connection for the given cookie and with
     * the given name.
     * @param cookie The cookie representing the client
     * @param name The name of the connection
     * @param flags The DCP open flags (as per protocol)
     * @param jsonExtra An optional JSON document for additional configuration
     */
    DcpProducer* newProducer(const void* cookie,
                             const std::string& name,
                             uint32_t flags,
                             Collections::Filter filter = {{/*no json*/},
                                                           nullptr});

    /**
     * Create a new consumer and add it in the list of TapConnections
     * @param e the engine
     * @param c the cookie representing the client
     * @return Pointer to the new dcp connection
     */
    DcpConsumer *newConsumer(const void* cookie, const std::string &name);

    void notifyVBConnections(uint16_t vbid, uint64_t bySeqno);

    void notifyBackfillManagerTasks();

    void removeVBConnections(DcpProducer& prod);

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

    /**
     * Close outbound (active) streams for a vbucket on vBucket rollback.
     *
     * @param vbucket the vbucket id
     */
    void closeStreamsDueToRollback(uint16_t vbucket);

    void shutdownAllConnections();

    bool isDeadConnectionsEmpty() {
        LockHolder lh(connsLock);
        return deadConnections.empty();
    }

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
        std::lock_guard<std::mutex> lh(backfills.mutex);
        return backfills.numActiveSnoozing;
    }

    uint16_t getMaxActiveSnoozingBackfills () {
        std::lock_guard<std::mutex> lh(backfills.mutex);
        return backfills.maxActiveSnoozing;
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

    std::shared_ptr<ConnHandler> findByName(const std::string& name);

    bool isConnections() {
        LockHolder lh(connsLock);
        return !map_.empty();
    }

    /**
     * Call a function on each DCP connection.
     */
    template <typename Fun>
    void each(Fun f) {
        LockHolder lh(connsLock);
        for (auto& c : map_) {
            f(c.second);
        }
    }

protected:
    /*
     * deadConnections is protected (as opposed to private) because
     * of the module test ep-engine_dead_connections_test
     */
    std::list<std::shared_ptr<ConnHandler>> deadConnections;

    /*
     * Change the value at which a DcpConsumer::Processor task will yield
     */
    void consumerYieldConfigChanged(size_t newValue);

    /*
     * Change the batchsize that the DcpConsumer::Processor operates with
     */
    void consumerBatchSizeConfigChanged(size_t newValue);

    bool isPassiveStreamConnected_UNLOCKED(uint16_t vbucket);

    /*
     * Closes all streams associated with each connection in `map`.
     */
    static void closeStreams(CookieToConnectionMap& map);

    /*
     * Cancels all tasks assocuated with each connection in `map`.
     */
    static void cancelTasks(CookieToConnectionMap& map);

    /* Db file memory */
    static const uint32_t dbFileMem;

    // Current and maximum number of backfills which are snoozing.
    struct {
        std::mutex mutex;
        uint16_t numActiveSnoozing;
        uint16_t maxActiveSnoozing;
    } backfills;

    /* Max num of backfills we want to have irrespective of memory */
    static const uint16_t numBackfillsThreshold;
    /* Max percentage of memory we want backfills to occupy */
    static const uint8_t numBackfillsMemThreshold;

    std::atomic<float> minCompressionRatioForProducer;

    /* Total memory used by all DCP consumer buffers */
    std::atomic<size_t> aggrDcpConsumerBufferSize;

    class DcpConfigChangeListener;
};
