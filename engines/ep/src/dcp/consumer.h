/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2013-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#pragma once

#include "atomic_unordered_map.h"

#include "connhandler.h"
#include "dcp/dcp-types.h"
#include "dcp/flow-control.h"
#include "ep_task.h"
#include "ep_types.h"
#include "vb_ready_queue.h"

#include <collections/collections_types.h>
#include <folly/Synchronized.h>
#include <memcached/dcp_stream_id.h>
#include <relaxed_atomic.h>

#include <list>
#include <map>
#include <utility>

class DcpResponse;
class PassiveStream;
class StreamEndResponse;
class UpdateFlowControl;

/**
 * A DCP Consumer object represents a DCP connection which receives streams
 * of mutations from another source and ingests those mutations.
 */
class DcpConsumer : public ConnHandler {
    using opaque_map = std::map<uint32_t, std::pair<uint32_t, Vbid>>;

public:
    /**
     * Each DCP control negotiation with the producer uses an instance of this
     * object, allowing send of the control and process of the response.
     * The response from the producer uses the success or failure callbacks to
     * allow other state to change.
     */
    struct BlockingDcpControlNegotiation;
    using Controls = std::deque<BlockingDcpControlNegotiation>;
    struct BlockingDcpControlNegotiation {
        /**
         * Construct with default success and failure callbacks that do nothing
         */
        BlockingDcpControlNegotiation(std::string_view key, std::string value)
            : key(key),
              value(std::move(value)),
              success([]() {}),
              failure([](Controls&) { return false; }) {
        }

        /**
         * Construct with a success callback, the failure callback is intialised
         * to do nothing
         */
        BlockingDcpControlNegotiation(std::string_view key,
                                      std::string value,
                                      std::function<void()> success)

            : key(key),
              value(std::move(value)),
              success(std::move(success)),
              failure([](Controls&) { return false; }) {
        }

        /**
         * Construct with a success and failure callback
         */
        BlockingDcpControlNegotiation(std::string_view key,
                                      std::string value,
                                      std::function<void()> success,
                                      std::function<bool(Controls&)> failure)
            : key(key),
              value(std::move(value)),
              success(std::move(success)),
              failure(std::move(failure)) {
        }

        // Used to identify the specific response from Producer. This is
        // optional so we can determine when the opaque has been assigned and
        // can ignore the response if it's from say the flow-control setup.
        std::optional<uint32_t> opaque;

        // The control key. e.g. "change_streams"
        std::string key;

        // The control value. e.g. "true"
        std::string value;

        // Callback for when the producer responds with success
        std::function<void()> success;

        // Callback for when the producer responds !success. This callback can
        // then control if the consumer should disconnect by returning true.
        // This callback takes a Controls (container) reference to which it can
        // emplace_back new controls.
        std::function<bool(Controls&)> failure;
    };

    /**
     * Construct a DCP consumer object.
     *
     * @param e Engine which owns this consumer.
     * @param cookie memcached cookie associated with this DCP consumer.
     * @param name The name of the connection.
     * @param consumerName (Optional) consumer_name; if non-empty used by the
     *        consumer to identify itself to the producer (for Sync
     *        Replication).
     */
    DcpConsumer(EventuallyPersistentEngine& e,
                CookieIface* cookie,
                const std::string& name,
                std::string consumerName);

    ~DcpConsumer() override;

    const char *getType() const override { return "consumer"; };

    /*
     * Creates a PassiveStream.
     *
     * @param e Reference to the engine
     * @param consumer The consumer the new stream will belong to
     * @param name The name of the new stream
     * @param flags The DCP flags
     * @param opaque The stream opaque
     * @param vb The vbucket the stream belongs to
     * @param start_seqno The start sequence number of the stream
     * @param vb_uuid The uuid of the vbucket the stream belongs to
     * @param snap_start_seqno The snapshot start sequence number
     * @param snap_end_seqno The snapshot end sequence number
     * @param vb_high_seqno The last received sequence number
     * @param vb_manifest_uid The newest collections manifest uid
     *
     * @return a SingleThreadedRCPtr to the newly created PassiveStream.
     */
    virtual std::shared_ptr<PassiveStream> makePassiveStream(
            EventuallyPersistentEngine& e,
            std::shared_ptr<DcpConsumer> consumer,
            const std::string& name,
            cb::mcbp::DcpAddStreamFlag flags,
            uint32_t opaque,
            Vbid vb,
            uint64_t start_seqno,
            uint64_t vb_uuid,
            uint64_t snap_start_seqno,
            uint64_t snap_end_seqno,
            uint64_t vb_high_seqno,
            const Collections::ManifestUid vb_manifest_uid);

    cb::engine_errc addStream(uint32_t opaque,
                              Vbid vbucket,
                              cb::mcbp::DcpAddStreamFlag flags) override;

    cb::engine_errc closeStream(uint32_t opaque,
                                Vbid vbucket,
                                cb::mcbp::DcpStreamId sid = {}) override;

    cb::engine_errc streamEnd(uint32_t opaque,
                              Vbid vbucket,
                              cb::mcbp::DcpStreamEndStatus status) override;

    cb::engine_errc mutation(uint32_t opaque,
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
                             uint8_t nru) override;

    cb::engine_errc deletion(uint32_t opaque,
                             const DocKeyView& key,
                             cb::const_byte_buffer value,
                             uint8_t datatype,
                             uint64_t cas,
                             Vbid vbucket,
                             uint64_t by_seqno,
                             uint64_t rev_seqno) override;

    cb::engine_errc deletionV2(uint32_t opaque,
                               const DocKeyView& key,
                               cb::const_byte_buffer value,
                               uint8_t datatype,
                               uint64_t cas,
                               Vbid vbucket,
                               uint64_t by_seqno,
                               uint64_t rev_seqno,
                               uint32_t delete_time) override;

    cb::engine_errc expiration(uint32_t opaque,
                               const DocKeyView& key,
                               cb::const_byte_buffer value,
                               uint8_t datatype,
                               uint64_t cas,
                               Vbid vbucket,
                               uint64_t by_seqno,
                               uint64_t rev_seqno,
                               uint32_t deleteTime) override;

    cb::engine_errc snapshotMarker(
            uint32_t opaque,
            Vbid vbucket,
            uint64_t start_seqno,
            uint64_t end_seqno,
            cb::mcbp::request::DcpSnapshotMarkerFlag flags,
            std::optional<uint64_t> high_completed_seqno,
            std::optional<uint64_t> high_prepared_seqno,
            std::optional<uint64_t> max_visible_seqno,
            std::optional<uint64_t> purge_seqno) override;

    cb::engine_errc noop(uint32_t opaque) override;

    cb::engine_errc setVBucketState(uint32_t opaque,
                                    Vbid vbucket,
                                    vbucket_state_t state) override;

    cb::engine_errc cached_value(uint32_t opaque,
                                 const DocKeyView& key,
                                 cb::const_byte_buffer value,
                                 uint8_t datatype,
                                 uint64_t cas,
                                 Vbid vbucket,
                                 uint32_t flags,
                                 uint64_t bySeqno,
                                 uint64_t revSeqno,
                                 uint32_t expiration,
                                 uint8_t nru) override;

    cb::engine_errc cached_key_meta(uint32_t opaque,
                                    const DocKeyView& key,
                                    uint8_t datatype,
                                    uint64_t cas,
                                    Vbid vbucket,
                                    uint32_t flags,
                                    uint64_t bySeqno,
                                    uint64_t revSeqno,
                                    uint32_t expiration) override;

    cb::engine_errc step(bool throttled,
                         DcpMessageProducersIface& producers) override;

    /**
     * Sub-classes must implement a method that processes a response
     * to a request initiated by itself.
     *
     * @param resp A mcbp response message to process.
     * @returns true/false which will be converted to SUCCESS/DISCONNECT by the
     *          engine.
     */
    bool handleResponse(const cb::mcbp::Response& resp) override;

    /**
     * Push a systemEvent onto this DCP consumer for consumption by a VB
     *
     * @param opaque The opaque for the stream.
     * @param vbucket The vbucket the event is being sent to.
     * @param event The mcbp::systemevent::id value.
     * @param bySeqno The seqno of the event.
     * @param key The event's key.
     * @param eventData The event's specific data.
     */
    cb::engine_errc systemEvent(uint32_t opaque,
                                Vbid vbucket,
                                mcbp::systemevent::id event,
                                uint64_t bySeqno,
                                mcbp::systemevent::version version,
                                cb::const_byte_buffer key,
                                cb::const_byte_buffer eventData) override;

    cb::engine_errc prepare(uint32_t opaque,
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
                            cb::durability::Level level) override;

    cb::engine_errc commit(uint32_t opaque,
                           Vbid vbucket,
                           const DocKeyView& key,
                           uint64_t prepare_seqno,
                           uint64_t commit_seqno) override;

    cb::engine_errc abort(uint32_t opaque,
                          Vbid vbucket,
                          const DocKeyView& key,
                          uint64_t prepareSeqno,
                          uint64_t abortSeqno) override;

    bool doRollback(uint32_t opaque, Vbid vbid, uint64_t rollbackSeqno);

    /**
     * Send a SeqnoAck message over the PassiveStream for the given VBucket.
     *
     * @param vbid
     * @param seqno The payload
     */
    void seqnoAckStream(Vbid vbid, int64_t seqno);

    void addStats(const AddStatFn& add_stat, CookieIface& c) override;

    void addStreamStats(const AddStatFn& add_stat,
                        CookieIface& c,
                        StreamStatsFormat format) override;

    void aggregateQueueStats(ConnCounter& aggregator) const override;

    void notifyStreamReady(Vbid vbucket);

    void closeAllStreams();

    void closeStreamDueToVbStateChange(Vbid vbucket, vbucket_state_t state);

    /**
     * Observes the memory state of the node and triggers buffer-ack of unacked
     * DCP bytes when the system recovers from OOM.
     *
     * @return ProcessUnackedBytesResult, see struct definition for details
     */
    ProcessUnackedBytesResult processUnackedBytes();

    uint32_t incrOpaqueCounter();

    size_t getFlowControlBufSize() const;

    void setFlowControlBufSize(size_t newSize);

    bool isStreamPresent(Vbid vbucket);

    void cancelTask();

    void taskCancelled();

    bool notifiedProcessor(bool to);

    void setProcessorTaskState(enum ProcessUnackedBytesResult to);

    std::string getProcessorTaskStatusStr() const;

    /**
     * Check if the enough bytes have been removed from the flow control
     * buffer, for the consumer to send an ACK back to the producer.
     * If so notify the front-end that this paused connection should be
     * woken-up.
     */
    void scheduleNotifyIfNecessary();

    void setDisconnect() override;

    void setAllowSanitizeValueInDeletion(bool value) {
        allowSanitizeValueInDeletion.store(value);
    }

    bool isAllowSanitizeValueInDeletion() {
        return allowSanitizeValueInDeletion.load();
    }

    /**
     * @return Whether FlowControl is enabled on this consumer connection
     */
    bool isFlowControlEnabled() const;

    void incrFlowControlFreedBytes(size_t bytes);

    /**
     * Adds to the pendingControl container the control to change the flow
     * control buffer size.
     * @param bufferSize the value to send to the producer
     */
    void addBufferSizeControl(size_t bufferSize);

    /**
     * @return true if our producer supports cache transfer.
     */
    bool isCacheTransferAvailable() const {
        return cacheTransfer;
    }

protected:
    /**
     * Records when the consumer last received a message from producer.
     * It is used to detect dead connections. The connection is closed
     * if a message, including a No-Op message, is not seen in a
     * specified time period.
     * It is protected so we can access from MockDcpConsumer, for
     * for testing purposes.
     */
    cb::time::steady_clock::time_point lastMessageTime;

    // Searches the streams map for a stream for vbucket ID. Returns the found
    // stream, or an empty pointer if none found.
    std::shared_ptr<PassiveStream> findStream(Vbid vbid);

    std::unique_ptr<DcpResponse> getNextItem();

    /**
     * Check if the provided opaque id is one of the
     * current open "session" id's
     *
     * @param opaque the provided opaque
     * @param vbucket the provided vbucket
     * @return true if the session is open, false otherwise
     */
    bool isValidOpaque(uint32_t opaque, Vbid vbucket);

    void streamAccepted(uint32_t opaque,
                        cb::mcbp::Status status,
                        cb::const_byte_buffer newFailoverLog);

    /*
     * Sends a GetErrorMap request to the other side
     *
     * @param producers Pointers to message producers
     *
     * @return cb::engine_errc::failed if the step has completed,
     * cb::engine_errc::success otherwise
     */
    cb::engine_errc handleGetErrorMap(DcpMessageProducersIface& producers);

    cb::engine_errc handleNoop(DcpMessageProducersIface& producers);

    /**
     * When pendingControls is not empty ::step() must drive DCP control
     * negotiations forwards. This function will look at the given control and
     * "step forward" the control negoiations based on the objects state.
     *
     * @param producers call control on this object if a message must be sent
     * @param control the current control state object
     * @return return value of producers.control or would_block
     */
    cb::engine_errc stepControlNegotiation(
            DcpMessageProducersIface& producers,
            BlockingDcpControlNegotiation& control);

    void notifyVbucketReady(Vbid vbucket);

    /**
     * This function is called when an addStream command gets a rollback
     * error from the producer.
     *
     * The function will either trigger a rollback to rollbackSeqno or
     * trigger the request of a new stream using the next (older) failover table
     * entry.
     *
     * @param vbid The vbucket the response is for.
     * @param opaque Unique handle for the stream's request/response.
     * @param rollbackSeqno The seqno to rollback to.
     *
     * @returns true/false which will be converted to SUCCESS/DISCONNECT by the
     *          engine.
     */
    bool handleRollbackResponse(Vbid vbid,
                                uint32_t opaque,
                                uint64_t rollbackSeqno);

    /**
     * The v2 and non v2 API are almost the same under the covers, so one
     * shared method handles both.
     */
    cb::engine_errc deletion(uint32_t opaque,
                             const DocKeyView& key,
                             cb::const_byte_buffer value,
                             uint8_t datatype,
                             uint64_t cas,
                             Vbid vbucket,
                             uint64_t bySeqno,
                             uint64_t revSeqno,
                             uint32_t deleteTime,
                             IncludeDeleteTime includeDeleteTime,
                             DeleteSource deletionCause,
                             UpdateFlowControl& ufc);

    /**
     * Helper function for mutation() and prepare() messages as they are handled
     * in a similar way.
     */
    cb::engine_errc processMutationOrPrepare(Vbid vbucket,
                                             uint32_t opaque,
                                             const DocKeyView& key,
                                             queued_item item,
                                             size_t baseMsgBytes);

    enum class DeleteType { Deletion, DeletionV2, Expiration };
    /**
     * With the new implementation of expiration, all three of deletion,
     * deletionV2 and expiration share identical code before slightly different
     * parameters into the above main deletion function, so this takes the
     * wrapping away from the original trio of functions.
     *
     * @param isV2DeleteOrExpiry An enum to identify the source and determine
     *                           whether to use v2 parameters or not.
     */
    cb::engine_errc toMainDeletion(DeleteType origin,
                                   uint32_t opaque,
                                   const DocKeyView& key,
                                   cb::const_byte_buffer value,
                                   uint8_t datatype,
                                   uint64_t cas,
                                   Vbid vbucket,
                                   uint64_t bySeqno,
                                   uint64_t revSeqno,
                                   uint32_t deleteTime);

    /**
     * Register a stream to this Consumer and add the VB-to-Consumer
     * mapping into DcpConnMap.
     *
     * @param stream The PassiveStream
     */
    void registerStream(std::shared_ptr<PassiveStream> stream);

    /**
     * Remove a stream from this Consumer and remove the VB-to-Consumer
     * mapping from DcpConnMap.
     *
     * Returns the removed stream ptr, or an empty shared_ptr if it
     * was not found
     *
     * @param vbid The stream to be removed
     */
    std::shared_ptr<PassiveStream> removeStream(Vbid vbid);

    /**
     * Helper method to lookup the correct stream for the given
     * vbid / opaque pair, and then dispatch the message to that stream.
     */
    cb::engine_errc lookupStreamAndDispatchMessage(
            UpdateFlowControl& ufc,
            Vbid vbucket,
            uint32_t opaque,
            std::unique_ptr<DcpResponse> msg);

    /**
     * Helper function to return the STREAM_NOT_FOUND if v7 status codes are
     * enabled, otherwise cb::engine_errc::no_such_key
     */
    cb::engine_errc getNoStreamFoundErrorCode() const;

    /**
     * Helper function to return the cb::engine_errc::opaque_no_match if v7
     * status codes are enabled, otherwise cb::engine_errc::key_already_exists
     * @return
     */
    cb::engine_errc getOpaqueMissMatchErrorCode() const;

    /**
     * Adds to the given controls container the control to negotiate DCP noop
     * using a seconds granularity (needed when the producer does not support
     * fractional seconds).
     */
    void addNoopSecondsPendingControl(Controls& controls);

    /**
     * The container of DCP control negotiations. BlockingDcpControlNegotiation
     * objects are added by construction and can even be added by the success or
     * failure callbacks (added to back of the deque).
     * The container is processed after the consumer is connected to the
     * producer from front to back. The objects are removed once they are
     * processed (response from producer).
     *
     * The container is protected by a mutex for safe access via stats. The main
     * send/receieve code accessing the container should all be the same worker
     * thread.
     */
    folly::Synchronized<Controls, std::mutex> pendingControls;

    /// Opaque generator. uint32_t as that is the maxium opaque supported by the
    /// MCBP protocol.
    uint32_t opaqueCounter{0};
    size_t processorTaskId;
    std::atomic<enum ProcessUnackedBytesResult> processorTaskState;

    /**
     * Queue of vbuckets which needed to "buffer" items (stop acking DCP
     * messages until the system memory usage allows these items to be
     * processed).
     *
     * Stats are generated with the prefix "dcp_buffered_ready_queue_".
     */
    VBReadyQueue bufferedVBQueue;
    std::atomic<bool> processorNotification;

    /**
     * Queue of vbuckets which have streams with items ready to pick up.
     */
    folly::Synchronized<std::list<Vbid>, std::mutex> readyStreamsVBQueue;

    // Map of vbid -> passive stream. Map itself is atomic (thread-safe).
    using PassiveStreamMap =
            AtomicUnorderedMap<Vbid, std::shared_ptr<PassiveStream>>;
    PassiveStreamMap streams;

    /*
     * Each time a stream is added an entry is made into the opaqueMap, which
     * maps a local opaque to a tuple of an externally provided opaque and vbid.
     */
    opaque_map opaqueMap_;

    cb::RelaxedAtomic<uint32_t> backoffs;
    // The interval that the consumer tells the producer to send noops
    std::chrono::duration<float> dcpNoopTxInterval;

    // Step can't start sending packets until we've received add stream
    cb::RelaxedAtomic<bool> pendingAddStream = true;

    // Flag to state that the DCP consumer has negotiated with the producer
    // that V7 DCP status codes can be used.
    bool useDcpV7StatusCodes = false;

    // True if the DCP consumer has negotiated with the producer that cache
    // transfer is available.
    bool cacheTransfer = false;

    // Sync Replication: The identifier the consumer should to identify itself
    // to the producer.
    const std::string consumerName;

    /*
     * MB-29441: The following variables are used to set the the proper
     * noop-interval on the producer depending on the producer version:
     * 1) if state::PendingRequest, then the consumer sends a GetErrorMap
     *     request to the producer
     * 2) we wait until state!=PendingResponse (i.e., response ready)
     * 3) the GetErrorMap command is available from version >= 5.0.0, so
     *     - if GetErrorMap succeeds, we have >= 5.0.0 producer and continue.
     *     - if GetErrorMap fails, we have <= 5.0.0 producer and disconnect.
     */
    enum class GetErrorMapState : uint8_t {
        Skip = 0, // Covers "do not send request" and "response ready"
        PendingRequest,
        PendingResponse
    } getErrorMapState{GetErrorMapState::Skip};

    /* Indicates if the 'Processor' task is running */
    std::atomic<bool> processorTaskRunning;

    FlowControl flowControl;

    /**
     * Whether this consumer should just sanitize invalid payloads in deletions
     * or fail the operation if an invalid payload is detected.
     * Non-const as the related configuration param is dynamic.
     */
    std::atomic_bool allowSanitizeValueInDeletion;

    friend UpdateFlowControl;
};

/**
 * RAII helper class to update the flowControl object with the number of
 * bytes to free and trigger the consumer notify
 */
class UpdateFlowControl {
public:
    UpdateFlowControl(DcpConsumer& consumer, size_t bytes)
        : consumer(consumer), bytes(bytes) {
        if (bytes == 0) {
            throw std::invalid_argument("UpdateFlowControl given 0 bytes");
        }
    }

    ~UpdateFlowControl() {
        if (bytes) {
            auto& ctl = consumer.flowControl;
            if (ctl.isEnabled()) {
                ctl.incrFreedBytes(bytes);
                consumer.scheduleNotifyIfNecessary();
            }
        }
    }

    /**
     * Calling release means that this object will not update the FlowControl
     * instance when destructed.
     * @return The 'bytes' which the object was tracking
     */
    size_t release() {
        auto rv = bytes;
        bytes = 0;
        return rv;
    }

private:
    DcpConsumer& consumer;
    size_t bytes{0};
};

/*
 * Task that orchestrates rollback on Consumer,
 * runs in background.
 */
class RollbackTask : public EpTask {
public:
    RollbackTask(EventuallyPersistentEngine& e,
                 uint32_t opaque_,
                 Vbid vbid_,
                 uint64_t rollbackSeqno_,
                 std::shared_ptr<DcpConsumer> conn)
        : EpTask(e, TaskId::RollbackTask, 0, false),
          description("Running rollback task for " + vbid_.to_string()),
          engine(&e),
          opaque(opaque_),
          vbid(vbid_),
          rollbackSeqno(rollbackSeqno_),
          cons(std::move(conn)) {
    }

    std::string getDescription() const override {
        return description;
    }

    std::chrono::microseconds maxExpectedDuration() const override {
        // Little data on how long this typically takes (rare operation).
        // Somewhat arbitrary selection of 10s as being slow.
        return std::chrono::seconds(10);
    }

    bool run() override;

private:
    const std::string description;
    EventuallyPersistentEngine *engine;
    uint32_t opaque;
    Vbid vbid;
    uint64_t rollbackSeqno;
    std::shared_ptr<DcpConsumer> cons;
};
