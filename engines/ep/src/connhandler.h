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

#include <folly/Synchronized.h>
#include <memcached/dcp.h>
#include <memcached/dcp_stream_id.h>
#include <memcached/engine.h>
#include <memcached/engine_error.h>
#include <memcached/vbucket.h>
#include <platform/cb_time.h>
#include <relaxed_atomic.h>
#include <atomic>
#include <mutex>
#include <string>

// forward decl
class BucketLogger;
struct DocKeyView;
class EPStats;
class EventuallyPersistentEngine;
class Stream;

/**
 * Aggregator object to count stats.
 */
struct ConnCounter {
    ConnCounter& operator+=(const ConnCounter& other) {
        totalConns += other.totalConns;
        totalProducers += other.totalProducers;
        conn_activeStreams += other.conn_activeStreams;
        conn_passiveStreams += other.conn_passiveStreams;
        conn_queueFill += other.conn_queueFill;
        conn_backfillDisk += other.conn_backfillDisk;
        conn_backfillMemory += other.conn_backfillMemory;
        conn_queueDrain += other.conn_queueDrain;
        conn_totalBytes += other.conn_totalBytes;
        conn_totalUncompressedDataSize += other.conn_totalUncompressedDataSize;
        conn_queueRemaining += other.conn_queueRemaining;
        conn_queueBackoff += other.conn_queueBackoff;
        conn_queueMemory += other.conn_queueMemory;
        conn_paused += other.conn_paused;
        conn_unpaused += other.conn_unpaused;
        return *this;
    }

    size_t totalConns = 0;
    size_t totalProducers = 0;

    size_t conn_activeStreams = 0;
    size_t conn_passiveStreams = 0;
    size_t conn_queueFill = 0;
    /// Number of items backfilled from disk
    size_t conn_backfillDisk = 0;
    /// Number of items backfilled from memory (either because Ephemeral bucket,
    /// or EP bucket where item was resident and avoid avoid disk read.
    size_t conn_backfillMemory = 0;
    size_t conn_queueDrain = 0;
    size_t conn_totalBytes = 0;
    size_t conn_totalUncompressedDataSize = 0;
    size_t conn_queueRemaining = 0;
    size_t conn_queueBackoff = 0;
    size_t conn_queueMemory = 0;
    size_t conn_paused{0};
    size_t conn_unpaused{0};
};

class ConnHandler : public DcpConnHandlerIface,
                    public std::enable_shared_from_this<ConnHandler> {
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
                CookieIface* cookie,
                std::string name);

    /**
     * Destruction of a ConnHandler releases the cookie in the server API
     * allowing the front end to free it.
     */
    ~ConnHandler() override;

    virtual cb::engine_errc addStream(uint32_t opaque,
                                      Vbid vbucket,
                                      cb::mcbp::DcpAddStreamFlag flags);

    virtual cb::engine_errc closeStream(uint32_t opaque,
                                        Vbid vbucket,
                                        cb::mcbp::DcpStreamId sid);

    virtual cb::engine_errc streamEnd(uint32_t opaque,
                                      Vbid vbucket,
                                      cb::mcbp::DcpStreamEndStatus status);

    virtual cb::engine_errc mutation(uint32_t opaque,
                                     const DocKeyView& key,
                                     cb::const_byte_buffer value,
                                     uint8_t datatype,
                                     uint64_t cas,
                                     Vbid vbucket,
                                     uint32_t flags,
                                     uint64_t by_seqno,
                                     uint64_t rev_seqno,
                                     uint32_t expiration,
                                     uint32_t lock_time,
                                     uint8_t nru);

    virtual cb::engine_errc deletion(uint32_t opaque,
                                     const DocKeyView& key,
                                     cb::const_byte_buffer value,
                                     uint8_t datatype,
                                     uint64_t cas,
                                     Vbid vbucket,
                                     uint64_t by_seqno,
                                     uint64_t rev_seqno);

    virtual cb::engine_errc deletionV2(uint32_t opaque,
                                       const DocKeyView& key,
                                       cb::const_byte_buffer value,
                                       uint8_t datatype,
                                       uint64_t cas,
                                       Vbid vbucket,
                                       uint64_t by_seqno,
                                       uint64_t rev_seqno,
                                       uint32_t delete_time);

    virtual cb::engine_errc expiration(uint32_t opaque,
                                       const DocKeyView& key,
                                       cb::const_byte_buffer value,
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
            cb::mcbp::request::DcpSnapshotMarkerFlag flags,
            std::optional<uint64_t> high_completed_seqno,
            std::optional<uint64_t> high_prepared_seqno,
            std::optional<uint64_t> max_visible_seqno,
            std::optional<uint64_t> purge_seqno);

    virtual cb::engine_errc setVBucketState(uint32_t opaque,
                                            Vbid vbucket,
                                            vbucket_state_t state);

    virtual cb::engine_errc streamRequest(cb::mcbp::DcpAddStreamFlag flags,
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
                                                  uint32_t buffer_bytes);

    virtual cb::engine_errc control(uint32_t opaque,
                                    std::string_view key,
                                    std::string_view value);

    virtual cb::engine_errc step(bool throttled,
                                 DcpMessageProducersIface& producers);

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
                                    const DocKeyView& key,
                                    cb::const_byte_buffer value,
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
                                   const DocKeyView& key,
                                   uint64_t prepare_seqno,
                                   uint64_t commit_seqno);

    /// Receive an abort message.
    virtual cb::engine_errc abort(uint32_t opaque,
                                  Vbid vbucket,
                                  const DocKeyView& key,
                                  uint64_t prepareSeqno,
                                  uint64_t abortSeqno);

    /// Receive a seqno_acknowledged message.
    virtual cb::engine_errc seqno_acknowledged(uint32_t opaque,
                                               Vbid vbucket,
                                               uint64_t prepared_seqno);

    /// Receive a cached_value message.
    virtual cb::engine_errc cached_value(uint32_t opaque,
                                         const DocKeyView& key,
                                         cb::const_byte_buffer value,
                                         uint8_t datatype,
                                         uint64_t cas,
                                         Vbid vbucket,
                                         uint32_t flags,
                                         uint64_t bySeqno,
                                         uint64_t revSeqno,
                                         uint32_t expiration,
                                         uint32_t lockTime,
                                         uint8_t nru);
    const char* logHeader() const;

    /**
     * Sets context which will be appended to all log lines.
     * @param header Prefixed to non-JSON logs
     * @param ctx Merged into JSON logs
     */
    void setLogContext(const std::string& header, const nlohmann::json& ctx);

    BucketLogger& getLogger();

    void setSupportAck(bool ack) {
        supportAck.store(ack);
    }

    virtual const char *getType() const = 0;

    template <typename T>
    void addStat(std::string_view nm,
                 const T& val,
                 const AddStatFn& add_stat,
                 CookieIface& c) const;

    /**
     * Generate statistics for this connection. Does not include per-stream
     * statistics.
     */
    virtual void addStats(const AddStatFn& add_stat, CookieIface& c);

    enum class StreamStatsFormat {
        /// Each stat in every stream is prefixed by the stream name and vbid.
        Legacy,
        /// All stats for every stream are exposed under the stream name and
        /// vbid as a single JSON string.
        Json,
    };

    /**
     * Generate statistics for all DCP streams associated with this connection.
     * @param add_stat Callback for sending stats.
     * @param c The associated cookie.
     * @param format The requested format.
     */
    virtual void addStreamStats(const AddStatFn& add_stat,
                                CookieIface& c,
                                StreamStatsFormat format) {
        // Empty
    }

    /**
     * Non-virtual overload for addStreamStats() which default format to Legacy.
     */
    void addStreamStats(const AddStatFn& add_stat, CookieIface& c) {
        addStreamStats(add_stat, c, StreamStatsFormat::Legacy);
    }

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

    CookieIface* getCookie() const {
        return cookie.load();
    }

    CookieIface* getCookie() {
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

    std::pair<size_t, size_t> getPausedCounters() const;

    void unPause();

    /**
     * @returns a JSON description of the current paused state, and details
     * of how previous paused reaons.
     */
    nlohmann::json getPausedDetailsDescription() const;

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

    /**
     * @return true if system-events are using FlatBuffers.
     */
    bool areFlatBuffersSystemEventsEnabled() const {
        return flatBuffersSystemEventsEnabled;
    }

    /**
     * @return true if the connection enables history.
     */
    bool areChangeStreamsEnabled() const {
        return changeStreams;
    }

protected:
    /**
     * Send stats for the given list of streams in the legacy format, where
     * every individual stream statistic features as a separate stat in the
     * output, prefixed by the stream name.
     */
    void doStreamStatsLegacy(
            const std::vector<std::shared_ptr<Stream>>& streams,
            const AddStatFn& add_stat,
            CookieIface& c);

    /**
     * Sends stats for the given list of streams in the JSON format, where every
     * stream is represented by a JSON object.
     */
    void doStreamStatsJson(const std::vector<std::shared_ptr<Stream>>& streams,
                           const AddStatFn& add_stat,
                           CookieIface& c);

    template <typename Derived>
    std::shared_ptr<Derived> shared_from_base() {
        return std::static_pointer_cast<Derived>(shared_from_this());
    }

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

    //! Connection creation time
    const cb::time::steady_clock::time_point created;

    /**
     * Flag used to state if we've received a control message with
     * "v7_dcp_status_codes" = "true".
     */
    cb::RelaxedAtomic<bool> enabledV7DcpStatus = false;

    /**
     * True if the sub-class has successfully enabled system events using
     * FlatBuffers structures. Producer/Consumer will set this once they have
     * processed a DCP control to enable.
     */
    std::atomic<bool> flatBuffersSystemEventsEnabled{false};

    /// Indicates whether this connection enables sending history on the streams
    std::atomic<bool> changeStreams = false;

    /**
     * Details of why and for how long a connection is paused, for diagnostic
     * purposes.
     */
    struct PausedDetails {
        /// Reason why the connection is currently paused.
        PausedReason reason{PausedReason::Unknown};
        /// When the connection was last paused.
        cb::time::steady_clock::time_point lastPaused{};
        /// Count of how many times the connection has previously been paused
        /// for each possible reason (excluding current pause)
        std::array<size_t, PausedReasonCount> reasonCounts{};
        /// Duration of how long the connection has previously been paused for
        /// each possible reason (excluding current pause).
        std::array<std::chrono::nanoseconds, PausedReasonCount>
                reasonDurations{};

        // The following two counters only change when the reason is
        // BufferLogFull and exist so we can remove logging of such events.
        size_t pausedCounter{0};
        size_t unpausedCounter{0};
    };

    [[nodiscard]] auto getPausedDetails() const {
        return pausedDetails.copy();
    }

private:
    //! The name for this connection
    std::string name;

    //! The cookie representing this connection (provided by the memcached code)
    std::atomic<CookieIface*> cookie;

    //! Should we disconnect as soon as possible?
    std::atomic<bool> disconnect;

    //! Whether or not this connection supports acking
    std::atomic<bool> supportAck;

    //! Connection is temporarily paused?
    std::atomic<bool> paused;

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
