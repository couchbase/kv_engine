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

#include "conn_store_fwd.h"
#include "connmap.h"
#include "ep_types.h"

#include <memcached/engine.h>
#include <folly/SharedMutex.h>
#include <atomic>
#include <list>
#include <string>

class CheckpointCursor;
class DcpProducer;
class DcpConsumer;

class DcpConnMap : public ConnMap {

public:

    DcpConnMap(EventuallyPersistentEngine &engine);

    ~DcpConnMap() override;

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
                             uint32_t flags);

    /**
     * Create a new consumer and add it in the list of DCP Connections
     *
     * @param cookie the cookie representing the client
     * @param name The name of the connection
     * @param consumerName (Optional) If non-empty an identifier for the
     *        consumer to advertise itself to the producer as.
     * @return Pointer to the new dcp connection
     */
    DcpConsumer* newConsumer(const void* cookie,
                             const std::string& name,
                             const std::string& consumerName = {});

    void notifyVBConnections(Vbid vbid,
                             uint64_t bySeqno,
                             SyncWriteOperation syncWrite);

    /**
     * Send a SeqnoAck message over the PassiveStream for the given VBucket.
     *
     * @param vbid
     * @param seqno The payload
     */
    void seqnoAckVBPassiveStream(Vbid vbid, int64_t seqno);

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
     * @param vbstateLock (optional) Exclusive lock to vbstate
     */
    void vbucketStateChanged(
            Vbid vbucket,
            vbucket_state_t state,
            bool closeInboundStreams = true,
            boost::optional<folly::SharedMutex::WriteHolder&> vbstateLock = {});

    /**
     * Close outbound (active) streams for a vbucket on vBucket rollback.
     *
     * @param vbucket the vbucket id
     */
    void closeStreamsDueToRollback(Vbid vbucket);

    void shutdownAllConnections();

    bool isDeadConnectionsEmpty() override {
        LockHolder lh(connsLock);
        return deadConnections.empty();
    }

    /**
     * Handles the slow stream with the specified name.
     * Returns true if the stream dropped its cursors on the
     * checkpoint.
     */
    bool handleSlowStream(Vbid vbid, const CheckpointCursor* cursor);

    void disconnect(const void *cookie);

    void manageConnections() override;

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

    ENGINE_ERROR_CODE addPassiveStream(ConnHandler& conn,
                                       uint32_t opaque,
                                       Vbid vbucket,
                                       uint32_t flags);

    /* Use this only for any quick direct stats from DcpConnMap. To collect
       individual conn stats from conn lists please use ConnStatBuilder */
    void addStats(const AddStatFn& add_stat, const void* c);

    /* Updates the minimum compression ratio to be achieved for docs by
     * all the producers, which will be in effect if the producer side
     * value compression is enabled */
    void updateMinCompressionRatioForProducers(float value);

    float getMinCompressionRatio();

    std::shared_ptr<ConnHandler> findByName(const std::string& name);

    bool isConnections() override;

    /**
     * Call a function on each DCP connection.
     */
    template <typename Fun>
    void each(Fun f);

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

    /**
     * Change the idle timeout that Producers and Consumers operate with
     */
    void idleTimeoutConfigChanged(size_t newValue);

    /**
     * @param engine The engine
     * @param cookie The cookie that identifies the connection
     * @param connName The name that identifies the connection
     * @param consumerName The name that identifies the consumer
     * @return a shared instance of DcpConsumer
     */
    virtual std::shared_ptr<DcpConsumer> makeConsumer(
            EventuallyPersistentEngine& engine,
            const void* cookie,
            const std::string& connName,
            const std::string& consumerName) const;

    bool isPassiveStreamConnected_UNLOCKED(Vbid vbucket);

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
