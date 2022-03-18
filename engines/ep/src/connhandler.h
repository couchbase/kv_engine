/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2015-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#pragma once

#include "dcp/dcp-types.h"
#include "utility.h"

#include <memcached/dcp.h>
#include <memcached/dcp_stream_id.h>
#include <memcached/engine.h>
#include <memcached/engine_error.h>
#include <memcached/vbucket.h>

#include <folly/Synchronized.h>
#include <atomic>
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
        : conn_queue(0),
          totalConns(0),
          totalProducers(0),
          conn_queueFill(0),
          conn_queueDrain(0),
          conn_totalBytes(0),
          conn_totalUncompressedDataSize(0),
          conn_queueRemaining(0),
          conn_queueBackoff(0),
          conn_queueItemOnDisk(0),
          conn_queueMemory(0) {
    }

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
        conn_queueMemory += other.conn_queueMemory;

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
    size_t conn_queueMemory;
};

class ConnHandler : public DcpConnHandlerIface {
public:
    enum class PausedReason : uint8_t {
        BufferLogFull,
        Initializing,
        OutOfMemory,
        ReadyListEmpty,
        Unknown
    };
    static constexpr size_t PausedReasonCount =
            size_t(PausedReason::Unknown) + 1;

    /**
     * Construction of a ConnHandler reserves the cookie in the server API
     * ensuring that it will not be released until we are done with it.
     *
     * @param engine reference
     * @param cookie for this connection
     * @param name of the connection
     */
    ConnHandler(EventuallyPersistentEngine& engine,
                const CookieIface* cookie,
                std::string name);

    /**
     * Destruction of a ConnHandler releases the cookie in the server API
     * allowing the front end to free it.
     */
    ~ConnHandler() override;

    virtual cb::engine_errc addStream(uint32_t opaque,
                                      Vbid vbucket,
                                      uint32_t flags);

    virtual cb::engine_errc closeStream(uint32_t opaque,
                                        Vbid vbucket,
                                        cb::mcbp::DcpStreamId sid);

    virtual cb::engine_errc streamEnd(uint32_t opaque,
                                      Vbid vbucket,
                                      cb::mcbp::DcpStreamEndStatus status);

    virtual cb::engine_errc mutation(uint32_t opaque,
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

    virtual cb::engine_errc deletion(uint32_t opaque,
                                     const DocKey& key,
                                     cb::const_byte_buffer value,
                                     size_t priv_bytes,
                                     uint8_t datatype,
                                     uint64_t cas,
                                     Vbid vbucket,
                                     uint64_t by_seqno,
                                     uint64_t rev_seqno,
                                     cb::const_byte_buffer meta);

    virtual cb::engine_errc deletionV2(uint32_t opaque,
                                       const DocKey& key,
                                       cb::const_byte_buffer value,
                                       size_t priv_bytes,
                                       uint8_t datatype,
                                       uint64_t cas,
                                       Vbid vbucket,
                                       uint64_t by_seqno,
                                       uint64_t rev_seqno,
                                       uint32_t delete_time);

    virtual cb::engine_errc expiration(uint32_t opaque,
                                       const DocKey& key,
                                       cb::const_byte_buffer value,
                                       size_t priv_bytes,
                                       uint8_t datatype,
                                       uint64_t cas,
                                       Vbid vbucket,
                                       uint64_t by_seqno,
                                       uint64_t rev_seqno,
                                       uint32_t deleteTime);

    virtual cb::engine_errc snapshotMarker(
            uint32_t opaque,
            Vbid vbucket,
            uint64_t start_seqno,
            uint64_t end_seqno,
            uint32_t flags,
            std::optional<uint64_t> high_completed_seqno,
            std::optional<uint64_t> max_visible_seqno);

    virtual cb::engine_errc setVBucketState(uint32_t opaque,
                                            Vbid vbucket,
                                            vbucket_state_t state);

    virtual cb::engine_errc streamRequest(uint32_t flags,
                                          uint32_t opaque,
                                          Vbid vbucket,
                                          uint64_t start_seqno,
                                          uint64_t end_seqno,
                                          uint64_t vbucket_uuid,
                                          uint64_t snapStartSeqno,
                                          uint64_t snapEndSeqno,
                                          uint64_t* rollback_seqno,
                                          dcp_add_failover_log callback,
                                          std::optional<std::string_view> json);

    virtual cb::engine_errc noop(uint32_t opaque);

    virtual cb::engine_errc bufferAcknowledgement(uint32_t opaque,
                                                  Vbid vbucket,
                                                  uint32_t buffer_bytes);

    virtual cb::engine_errc control(uint32_t opaque,
                                    std::string_view key,
                                    std::string_view value);

    virtual cb::engine_errc step(DcpMessageProducersIface& producers);

    /**
     * Sub-classes must implement a method that processes a response
     * to a request initiated by itself.
     *
     * @param resp A mcbp response message to process.
     * @returns true/false which will be converted to SUCCESS/DISCONNECT by the
     *          engine.
     */
    virtual bool handleResponse(const cb::mcbp::Response& resp);

    virtual cb::engine_errc systemEvent(uint32_t opaque,
                                        Vbid vbucket,
                                        mcbp::systemevent::id event,
                                        uint64_t bySeqno,
                                        mcbp::systemevent::version version,
                                        cb::const_byte_buffer key,
                                        cb::const_byte_buffer eventData);

    /// Receive a prepare message.
    virtual cb::engine_errc prepare(uint32_t opaque,
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
                                    uint8_t nru,
                                    DocumentState document_state,
                                    cb::durability::Level level);

    /// Receive a commit message.
    virtual cb::engine_errc commit(uint32_t opaque,
                                   Vbid vbucket,
                                   const DocKey& key,
                                   uint64_t prepare_seqno,
                                   uint64_t commit_seqno);

    /// Receive an abort message.
    virtual cb::engine_errc abort(uint32_t opaque,
                                  Vbid vbucket,
                                  const DocKey& key,
                                  uint64_t prepareSeqno,
                                  uint64_t abortSeqno);

    /// Receive a seqno_acknowledged message.
    virtual cb::engine_errc seqno_acknowledged(uint32_t opaque,
                                               Vbid vbucket,
                                               uint64_t prepared_seqno);

    const char* logHeader();

    void setLogHeader(const std::string& header);

    BucketLogger& getLogger();

    void setSupportAck(bool ack) {
        supportAck.store(ack);
    }

    virtual const char *getType() const = 0;

    template <typename T>
    void addStat(const char* nm,
                 const T& val,
                 const AddStatFn& add_stat,
                 const void* c) const;

    virtual void addStats(const AddStatFn& add_stat, const CookieIface* c);

    virtual void aggregateQueueStats(ConnCounter& stats_aggregator) const {
        // Empty
    }

    /**
     * Does the Connection support SyncReplication (Acking prepares)?
     */
    bool isSyncReplicationEnabled() const {
        return supportsSyncReplication.load() ==
               SyncReplication::SyncReplication;
    }

    /**
     * Does the Connection support SyncWrites (sending and receiving Prepares,
     * Commits, and Aborts)?
     */
    bool isSyncWritesEnabled() const {
        return supportsSyncReplication.load() != SyncReplication::No;
    }

    SyncReplication getSyncReplSupport() const {
        return supportsSyncReplication.load();
    }

    const std::string &getName() const {
        return name;
    }

    const CookieIface* getCookie() const {
        return cookie.load();
    }

    bool doDisconnect() {
        return disconnect.load();
    }

    virtual void setDisconnect() {
        flagDisconnect();
    }

    /**
     * Just flags this connection as disconnected.
     */
    void flagDisconnect() {
        disconnect.store(true);
    }

    // Pause the connection.
    // @param reason why the connection was paused - for debugging / diagnostic
    void pause(PausedReason r = PausedReason::Unknown);

    PausedReason getPausedReason() const {
        return pausedDetails.lock()->reason;
    }

    [[nodiscard]] bool isPaused() const {
        return paused;
    }

    void unPause();

    const std::string& getAuthenticatedUser() const {
        return authenticatedUser;
    }

    in_port_t getConnectedPort() const {
        return connected_port;
    }

    void setIdleTimeout(std::chrono::seconds newValue) {
        idleTimeout = newValue;
    }

    std::chrono::seconds getIdleTimeout() {
        return idleTimeout;
    }

    /**
     * Notifies the front-end asynchronously that this paused
     * connection should be re-considered for work.
     */
    void scheduleNotify();

protected:
    EventuallyPersistentEngine &engine_;
    EPStats &stats;

    //! The bucketLogger for this connection
    std::shared_ptr<BucketLogger> logger;

    /**
     * Does this DCP Connection support Synchronous Replication (i.e. acking
     * Prepares). A connection should support SyncWrites to support
     * SyncReplication.
     */
    std::atomic<SyncReplication> supportsSyncReplication{SyncReplication::No};

    /**
     * Indicates whether this DCP connection supports streaming user-xattrs in
     * the value for normal and sync DCP delete.
     */
    IncludeDeletedUserXattrs includeDeletedUserXattrs{
            IncludeDeletedUserXattrs::No};

    /**
     * Flag used to state if we've received a control message with
     * "v7_dcp_status_codes" = "true".
     */
    bool enabledV7DcpStatus = false;

private:

     //! The name for this connection
    std::string name;

    //! The cookie representing this connection (provided by the memcached code)
    std::atomic<CookieIface*> cookie;

    //! Connection creation time
    const rel_time_t created;

    //! Should we disconnect as soon as possible?
    std::atomic<bool> disconnect;

    //! Whether or not this connection supports acking
    std::atomic<bool> supportAck;

    //! Connection is temporarily paused?
    std::atomic<bool> paused;

    /**
     * Details of why and for how long a connection is paused, for diagnostic
     * purposes.
     */
    struct PausedDetails {
        /// Reason why the connection is currently paused.
        PausedReason reason{PausedReason::Unknown};
        /// When the connection was last paused.
        std::chrono::steady_clock::time_point lastPaused{};
        /// Count of how many times the connection has previously been paused
        /// for each possible reason (excluding current pause)
        std::array<size_t, PausedReasonCount> reasonCounts{};
        /// Duration of how long the connection has previously been paused for
        /// each possible reason (excluding current pause).
        std::array<std::chrono::nanoseconds, PausedReasonCount>
                reasonDurations{};
    };
    folly::Synchronized<PausedDetails, std::mutex> pausedDetails;

    /// The authenticated user the connection
    const std::string authenticatedUser;

    /// The port the connection is connected to
    const in_port_t connected_port;

    /**
     * A timeout value after which we will disconnect the connection if no
     * message has been received (provided noopCtx has been enabled).
     */
    std::chrono::seconds idleTimeout;
};

std::string to_string(ConnHandler::PausedReason r);
