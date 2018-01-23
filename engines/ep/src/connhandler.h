/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2015 Couchbase, Inc
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

#include <list>
#include <map>
#include <queue>
#include <set>
#include <string>
#include <vector>

#include "kv_bucket_iface.h"
#include "logger.h"
#include "statwriter.h"
#include "utility.h"
#include "vb_filter.h"
#include "vbucket.h"

// forward decl
class EventuallyPersistentEngine;


/**
 * Aggregator object to count stats.
 */
struct ConnCounter {
    ConnCounter()
        : conn_queue(0), totalConns(0), totalProducers(0),
          conn_queueFill(0), conn_queueDrain(0), conn_totalBytes(0),
          conn_totalUncompressedDataSize(0), conn_queueRemaining(0),
          conn_queueBackoff(0), conn_queueItemOnDisk(0)
    {}

    ConnCounter& operator+=(const ConnCounter& other) {
        conn_queue += other.conn_queue;
        totalConns += other.totalConns;
        totalProducers += other.totalProducers;
        conn_queueFill += other.conn_queueFill;
        conn_queueDrain += other.conn_queueDrain;
        conn_totalBytes += other.conn_totalBytes;
        conn_totalUncompressedDataSize += other.conn_totalUncompressedDataSize;
        conn_queueRemaining += other.conn_queueRemaining;
        conn_queueBackoff += other.conn_queueBackoff;
        conn_queueItemOnDisk += other.conn_queueItemOnDisk;

        return *this;
    }

    size_t      conn_queue;
    size_t      totalConns;
    size_t      totalProducers;

    size_t      conn_queueFill;
    size_t      conn_queueDrain;
    size_t      conn_totalBytes;
    size_t      conn_totalUncompressedDataSize;
    size_t      conn_queueRemaining;
    size_t      conn_queueBackoff;
    size_t      conn_queueItemOnDisk;
};

class ConnHandler {
public:
    ConnHandler(EventuallyPersistentEngine& engine, const void* c,
                const std::string& name);

    virtual ~ConnHandler() {}

    virtual ENGINE_ERROR_CODE addStream(uint32_t opaque, uint16_t vbucket,
                                        uint32_t flags);

    virtual ENGINE_ERROR_CODE closeStream(uint32_t opaque, uint16_t vbucket);

    virtual ENGINE_ERROR_CODE streamEnd(uint32_t opaque, uint16_t vbucket,
                                        uint32_t flags);

    virtual ENGINE_ERROR_CODE mutation(uint32_t opaque,
                                       const DocKey& key,
                                       cb::const_byte_buffer value,
                                       size_t priv_bytes,
                                       uint8_t datatype,
                                       uint64_t cas,
                                       uint16_t vbucket,
                                       uint32_t flags,
                                       uint64_t by_seqno,
                                       uint64_t rev_seqno,
                                       uint32_t expiration,
                                       uint32_t lock_time,
                                       cb::const_byte_buffer meta,
                                       uint8_t nru);

    virtual ENGINE_ERROR_CODE deletion(uint32_t opaque,
                                       const DocKey& key,
                                       cb::const_byte_buffer value,
                                       size_t priv_bytes,
                                       uint8_t datatype,
                                       uint64_t cas,
                                       uint16_t vbucket,
                                       uint64_t by_seqno,
                                       uint64_t rev_seqno,
                                       cb::const_byte_buffer meta);

    virtual ENGINE_ERROR_CODE deletionV2(uint32_t opaque,
                                         const DocKey& key,
                                         cb::const_byte_buffer value,
                                         size_t priv_bytes,
                                         uint8_t datatype,
                                         uint64_t cas,
                                         uint16_t vbucket,
                                         uint64_t by_seqno,
                                         uint64_t rev_seqno,
                                         uint32_t delete_time);

    virtual ENGINE_ERROR_CODE expiration(uint32_t opaque,
                                         const DocKey& key,
                                         cb::const_byte_buffer value,
                                         size_t priv_bytes,
                                         uint8_t datatype,
                                         uint64_t cas,
                                         uint16_t vbucket,
                                         uint64_t by_seqno,
                                         uint64_t rev_seqno,
                                         cb::const_byte_buffer meta);

    virtual ENGINE_ERROR_CODE snapshotMarker(uint32_t opaque,
                                             uint16_t vbucket,
                                             uint64_t start_seqno,
                                             uint64_t end_seqno,
                                             uint32_t flags);

    virtual ENGINE_ERROR_CODE flushall(uint32_t opaque, uint16_t vbucket);

    virtual ENGINE_ERROR_CODE setVBucketState(uint32_t opaque, uint16_t vbucket,
                                              vbucket_state_t state);

    virtual ENGINE_ERROR_CODE getFailoverLog(uint32_t opaque, uint16_t vbucket,
                                             dcp_add_failover_log callback);

    virtual ENGINE_ERROR_CODE streamRequest(uint32_t flags,
                                            uint32_t opaque,
                                            uint16_t vbucket,
                                            uint64_t start_seqno,
                                            uint64_t end_seqno,
                                            uint64_t vbucket_uuid,
                                            uint64_t snapStartSeqno,
                                            uint64_t snapEndSeqno,
                                            uint64_t *rollback_seqno,
                                            dcp_add_failover_log callback);

    virtual ENGINE_ERROR_CODE noop(uint32_t opaque);

    virtual ENGINE_ERROR_CODE bufferAcknowledgement(uint32_t opaque,
                                                    uint16_t vbucket,
                                                    uint32_t buffer_bytes);

    virtual ENGINE_ERROR_CODE control(uint32_t opaque, const void* key,
                                      uint16_t nkey, const void* value,
                                      uint32_t nvalue);

    virtual ENGINE_ERROR_CODE step(struct dcp_message_producers* producers);

    /**
     * Sub-classes must implement a method that processes a response
     * to a request initiated by itself.
     *
     * @param resp A mcbp response message to process.
     * @returns true/false which will be converted to SUCCESS/DISCONNECT by the
     *          engine.
     */
    virtual bool handleResponse(const protocol_binary_response_header* resp);

    virtual ENGINE_ERROR_CODE systemEvent(uint32_t opaque,
                                          uint16_t vbucket,
                                          mcbp::systemevent::id event,
                                          uint64_t bySeqno,
                                          cb::const_byte_buffer key,
                                          cb::const_byte_buffer eventData);

    const char* logHeader() {
        return logger.prefix.c_str();
    }

    void setLogHeader(const std::string &header) {
        logger.prefix = header;
    }

    const Logger& getLogger() const;

    void releaseReference();

    void setSupportAck(bool ack) {
        supportAck.store(ack);
    }

    virtual const char *getType() const = 0;

    template <typename T>
    void addStat(const char *nm, const T &val, ADD_STAT add_stat, const void *c) const {
        std::stringstream tap;
        tap << name << ":" << nm;
        std::stringstream value;
        value << val;
        std::string n = tap.str();
        add_casted_stat(n.data(), value.str().data(), add_stat, c);
    }

    void addStat(const char *nm, bool val, ADD_STAT add_stat, const void *c) const {
        addStat(nm, val ? "true" : "false", add_stat, c);
    }

    virtual void addStats(ADD_STAT add_stat, const void *c) {
        addStat("type", getType(), add_stat, c);
        addStat("created", created.load(), add_stat, c);
        addStat("pending_disconnect", disconnect.load(), add_stat, c);
        addStat("supports_ack", supportAck.load(), add_stat, c);
        addStat("reserved", reserved.load(), add_stat, c);
        addStat("paused", isPaused(), add_stat, c);
        if (isPaused()) {
            addStat("paused_reason", getPausedReason(), add_stat, c);
        }
    }

    virtual void aggregateQueueStats(ConnCounter& stats_aggregator) {
        // Empty
    }

    const std::string &getName() const {
        return name;
    }

    bool setReserved(bool r) {
        bool inverse = !r;
        return reserved.compare_exchange_strong(inverse, r);
    }

    bool isReserved() const {
        return reserved;
    }

    const void *getCookie() const {
        return cookie.load();
    }

    bool doDisconnect() {
        return disconnect.load();
    }

    virtual void setDisconnect() {
        disconnect.store(true);
    }

    // Pause the connection.
    // @param reason why the connection was paused - for debugging / diagnostic
    void pause(std::string reason = "unknown") {
        std::lock_guard<std::mutex> guard(pausedReason.mutex);
        paused.store(true);
        pausedReason.string = reason;
    }

    std::string getPausedReason() const {
        std::lock_guard<std::mutex> guard(pausedReason.mutex);
        return pausedReason.string;
    }

    bool isPaused() {
        return paused;
    }

    void unPause() {
        paused.store(false);
    }

protected:
    EventuallyPersistentEngine &engine_;
    EPStats &stats;

    //! The logger for this connection
    Logger logger;

private:

     //! The name for this connection
    std::string name;

    //! The cookie representing this connection (provided by the memcached code)
    std::atomic<void*> cookie;

    //! Whether or not the connection is reserved in the memcached layer
    std::atomic<bool> reserved;

    //! Connection creation time
    std::atomic<rel_time_t> created;

    //! Should we disconnect as soon as possible?
    std::atomic<bool> disconnect;

    //! Whether or not this connection supports acking
    std::atomic<bool> supportAck;

    //! Connection is temporarily paused?
    std::atomic<bool> paused;

    //! Description of why the connection is paused.
    struct pausedReason {
        mutable std::mutex mutex;
        std::string string;
    } pausedReason;
};
