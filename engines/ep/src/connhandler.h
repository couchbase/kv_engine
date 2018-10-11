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

#include "statwriter.h"
#include "utility.h"

#include <memcached/dcp.h>
#include <memcached/dcp_stream_id.h>
#include <memcached/engine.h>
#include <memcached/engine_error.h>
#include <memcached/vbucket.h>

#include <mutex>
#include <string>

// forward decl
class BucketLogger;
struct DocKey;
class EPStats;
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
    enum class PausedReason {
        BufferLogFull,
        Initializing,
        OutOfMemory,
        ReadyListEmpty,
        Unknown
    };

    ConnHandler(EventuallyPersistentEngine& engine, const void* c,
                const std::string& name);

    virtual ~ConnHandler() {}

    virtual ENGINE_ERROR_CODE addStream(uint32_t opaque,
                                        Vbid vbucket,
                                        uint32_t flags);

    virtual ENGINE_ERROR_CODE closeStream(uint32_t opaque,
                                          Vbid vbucket,
                                          DcpStreamId sid);

    virtual ENGINE_ERROR_CODE streamEnd(uint32_t opaque,
                                        Vbid vbucket,
                                        uint32_t flags);

    virtual ENGINE_ERROR_CODE mutation(uint32_t opaque,
                                       const DocKey& key,
                                       cb::const_byte_buffer value,
                                       size_t priv_bytes,
                                       uint8_t datatype,
                                       uint64_t cas,
                                       Vbid vbucket,
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
                                       Vbid vbucket,
                                       uint64_t by_seqno,
                                       uint64_t rev_seqno,
                                       cb::const_byte_buffer meta);

    virtual ENGINE_ERROR_CODE deletionV2(uint32_t opaque,
                                         const DocKey& key,
                                         cb::const_byte_buffer value,
                                         size_t priv_bytes,
                                         uint8_t datatype,
                                         uint64_t cas,
                                         Vbid vbucket,
                                         uint64_t by_seqno,
                                         uint64_t rev_seqno,
                                         uint32_t delete_time);

    virtual ENGINE_ERROR_CODE expiration(uint32_t opaque,
                                         const DocKey& key,
                                         cb::const_byte_buffer value,
                                         size_t priv_bytes,
                                         uint8_t datatype,
                                         uint64_t cas,
                                         Vbid vbucket,
                                         uint64_t by_seqno,
                                         uint64_t rev_seqno,
                                         uint32_t deleteTime);

    virtual ENGINE_ERROR_CODE snapshotMarker(uint32_t opaque,
                                             Vbid vbucket,
                                             uint64_t start_seqno,
                                             uint64_t end_seqno,
                                             uint32_t flags);

    virtual ENGINE_ERROR_CODE setVBucketState(uint32_t opaque,
                                              Vbid vbucket,
                                              vbucket_state_t state);

    virtual ENGINE_ERROR_CODE streamRequest(
            uint32_t flags,
            uint32_t opaque,
            Vbid vbucket,
            uint64_t start_seqno,
            uint64_t end_seqno,
            uint64_t vbucket_uuid,
            uint64_t snapStartSeqno,
            uint64_t snapEndSeqno,
            uint64_t* rollback_seqno,
            dcp_add_failover_log callback,
            boost::optional<cb::const_char_buffer> json);

    virtual ENGINE_ERROR_CODE noop(uint32_t opaque);

    virtual ENGINE_ERROR_CODE bufferAcknowledgement(uint32_t opaque,
                                                    Vbid vbucket,
                                                    uint32_t buffer_bytes);

    virtual ENGINE_ERROR_CODE control(uint32_t opaque,
                                      cb::const_char_buffer key,
                                      cb::const_char_buffer value);

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
                                          Vbid vbucket,
                                          mcbp::systemevent::id event,
                                          uint64_t bySeqno,
                                          mcbp::systemevent::version version,
                                          cb::const_byte_buffer key,
                                          cb::const_byte_buffer eventData);

    const char* logHeader();

    void setLogHeader(const std::string& header);

    BucketLogger& getLogger();

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

    virtual void addStats(ADD_STAT add_stat, const void* c);

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
    void pause(PausedReason r = PausedReason::Unknown) {
        paused.store(true);
        reason = r;
    }

    PausedReason getPausedReason() const {
        return reason;
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

    //! The bucketLogger for this connection
    std::shared_ptr<BucketLogger> logger;

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
    std::atomic<PausedReason> reason;
};

std::string to_string(ConnHandler::PausedReason r);
