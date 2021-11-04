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
#include "ep_types.h"
#include "vb_ready_queue.h"
#include <executor/globaltask.h>

#include <collections/collections_types.h>
#include <memcached/dcp_stream_id.h>
#include <relaxed_atomic.h>

#include <list>
#include <map>
#include <utility>

class DcpResponse;
class PassiveStream;
class StreamEndResponse;

/**
 * A DCP Consumer object represents a DCP connection which receives streams
 * of mutations from another source and ingests those mutations.
 */
class DcpConsumer : public ConnHandler,
                    public std::enable_shared_from_this<DcpConsumer> {
    using opaque_map = std::map<uint32_t, std::pair<uint32_t, Vbid>>;

public:
    /**
     * Some of the DCP Consumer/Producer negotiation happens over DcpControl and
     * it is blocking (eg, SyncReplication). An instance of this struct is used
     * to process a specific negotiation on the Consumer side.
     */
    struct BlockingDcpControlNegotiation {
        enum class State : uint8_t {
            PendingRequest,
            PendingResponse,
            Completed // Covers "nothing to negotiate" and "neg complete"
        } state;
        // Used to identify the specific response from Producer.
        uint32_t opaque{0};
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
                const CookieIface* cookie,
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
     * @param end_seqno The end sequence number of the stream
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
            uint32_t flags,
            uint32_t opaque,
            Vbid vb,
            uint64_t start_seqno,
            uint64_t end_seqno,
            uint64_t vb_uuid,
            uint64_t snap_start_seqno,
            uint64_t snap_end_seqno,
            uint64_t vb_high_seqno,
            const Collections::ManifestUid vb_manifest_uid);

    cb::engine_errc addStream(uint32_t opaque,
                              Vbid vbucket,
                              uint32_t flags) override;

    cb::engine_errc closeStream(uint32_t opaque,
                                Vbid vbucket,
                                cb::mcbp::DcpStreamId sid = {}) override;

    cb::engine_errc streamEnd(uint32_t opaque,
                              Vbid vbucket,
                              cb::mcbp::DcpStreamEndStatus status) override;

    cb::engine_errc mutation(uint32_t opaque,
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
                             uint8_t nru) override;

    cb::engine_errc deletion(uint32_t opaque,
                             const DocKey& key,
                             cb::const_byte_buffer value,
                             size_t priv_bytes,
                             uint8_t datatype,
                             uint64_t cas,
                             Vbid vbucket,
                             uint64_t by_seqno,
                             uint64_t rev_seqno,
                             cb::const_byte_buffer meta) override;

    cb::engine_errc deletionV2(uint32_t opaque,
                               const DocKey& key,
                               cb::const_byte_buffer value,
                               size_t priv_bytes,
                               uint8_t datatype,
                               uint64_t cas,
                               Vbid vbucket,
                               uint64_t by_seqno,
                               uint64_t rev_seqno,
                               uint32_t delete_time) override;

    cb::engine_errc expiration(uint32_t opaque,
                               const DocKey& key,
                               cb::const_byte_buffer value,
                               size_t priv_bytes,
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
            uint32_t flags,
            std::optional<uint64_t> high_completed_seqno,
            std::optional<uint64_t> max_visible_seqno) override;

    cb::engine_errc noop(uint32_t opaque) override;

    cb::engine_errc setVBucketState(uint32_t opaque,
                                    Vbid vbucket,
                                    vbucket_state_t state) override;

    cb::engine_errc step(DcpMessageProducersIface& producers) override;

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
                            cb::durability::Level level) override;

    cb::engine_errc commit(uint32_t opaque,
                           Vbid vbucket,
                           const DocKey& key,
                           uint64_t prepare_seqno,
                           uint64_t commit_seqno) override;

    cb::engine_errc abort(uint32_t opaque,
                          Vbid vbucket,
                          const DocKey& key,
                          uint64_t prepareSeqno,
                          uint64_t abortSeqno) override;

    cb::engine_errc control(uint32_t opaque,
                            std::string_view key,
                            std::string_view value) override;

    bool doRollback(uint32_t opaque, Vbid vbid, uint64_t rollbackSeqno);

    /**
     * Send a SeqnoAck message over the PassiveStream for the given VBucket.
     *
     * @param vbid
     * @param seqno The payload
     */
    void seqnoAckStream(Vbid vbid, int64_t seqno);

    void addStats(const AddStatFn& add_stat, const CookieIface* c) override;

    void aggregateQueueStats(ConnCounter& aggregator) const override;

    void notifyStreamReady(Vbid vbucket);

    void closeAllStreams();

    void closeStreamDueToVbStateChange(Vbid vbucket, vbucket_state_t state);

    process_items_error_t processBufferedItems();

    uint64_t incrOpaqueCounter();

    uint32_t getFlowControlBufSize();

    void setFlowControlBufSize(uint32_t newSize);

    static const std::string& getControlMsgKey();

    bool isStreamPresent(Vbid vbucket);

    void cancelTask();

    void taskCancelled();

    bool notifiedProcessor(bool to);

    void setProcessorTaskState(enum process_items_error_t to);

    std::string getProcessorTaskStatusStr() const;

    /**
     * Check if the enough bytes have been removed from the flow control
     * buffer, for the consumer to send an ACK back to the producer.
     * If so notify the front-end that this paused connection should be
     * woken-up.
     */
    void immediatelyNotifyIfNecessary();

    /**
     * Check if the enough bytes have been removed from the flow control
     * buffer, for the consumer to send an ACK back to the producer.
     * If so schedule a notification to the front-end that this paused
     * connection should be woken-up.
     */
    void scheduleNotifyIfNecessary();

    void setProcessorYieldThreshold(size_t newValue) {
        processBufferedMessagesYieldThreshold = newValue;
    }

    void setProcessBufferedMessagesBatchSize(size_t newValue) {
        processBufferedMessagesBatchSize = newValue;
    }

    /**
     * Notifies the front-end synchronously on this thread that this paused
     * connection should be re-considered for work.
     */
    void immediatelyNotify();

    /**
     * Schedule a notification to the front-end on a background thread for
     * the ConnNotifier to pick that notifies this paused connection should
     * be re-considered for work.
     */
    void scheduleNotify();

    void setDisconnect() override;

    void setAllowSanitizeValueInDeletion(bool value) {
        allowSanitizeValueInDeletion.store(value);
    }

    bool isAllowSanitizeValueInDeletion() {
        return allowSanitizeValueInDeletion.load();
    }

    /**
     * Force streams to buffer?
     * @return true if streams should always buffer operations
     */
    bool shouldBufferOperations() const;

protected:
    /**
     * Records when the consumer last received a message from producer.
     * It is used to detect dead connections. The connection is closed
     * if a message, including a No-Op message, is not seen in a
     * specified time period.
     * It is protected so we can access from MockDcpConsumer, for
     * for testing purposes.
     */
    rel_time_t lastMessageTime;

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
                        const uint8_t* body,
                        uint32_t bodylen);

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

    cb::engine_errc handlePriority(DcpMessageProducersIface& producers);

    cb::engine_errc supportCursorDropping(DcpMessageProducersIface& producers);

    cb::engine_errc supportHifiMFU(DcpMessageProducersIface& producers);

    cb::engine_errc sendStreamEndOnClientStreamClose(
            DcpMessageProducersIface& producers);

    cb::engine_errc enableExpiryOpcode(DcpMessageProducersIface& producers);

    cb::engine_errc enableSynchronousReplication(
            DcpMessageProducersIface& producers);

    cb::engine_errc enableV7DcpStatus(DcpMessageProducersIface& producers);

    /**
     * Handles the negotiation of IncludeDeletedUserXattrs.
     *
     * @param producers Pointers to message producers
     */
    cb::engine_errc handleDeletedUserXattrs(
            DcpMessageProducersIface& producers);

    void notifyVbucketReady(Vbid vbucket);

    /**
     * Drain the stream of bufferedItems
     * The function will stop draining
     *  - if there's no more data - all_processed
     *  - if the replication throttle says no more - cannot_process
     *  - if there's an error, e.g. ETMPFAIL/ENOMEM - cannot_process
     *  - if we hit the yieldThreshold - more_to_process
     */
    process_items_error_t drainStreamsBufferedItems(
            std::shared_ptr<PassiveStream> stream, size_t yieldThreshold);

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
                             const DocKey& key,
                             cb::const_byte_buffer value,
                             uint8_t datatype,
                             uint64_t cas,
                             Vbid vbucket,
                             uint64_t bySeqno,
                             uint64_t revSeqno,
                             cb::const_byte_buffer meta,
                             uint32_t deleteTime,
                             IncludeDeleteTime includeDeleteTime,
                             DeleteSource deletionCause);

    /**
     * Helper function for mutation() and prepare() messages as they are handled
     * in a similar way.
     */
    cb::engine_errc processMutationOrPrepare(Vbid vbucket,
                                             uint32_t opaque,
                                             const DocKey& key,
                                             queued_item item,
                                             cb::const_byte_buffer meta,
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
                                   const DocKey& key,
                                   cb::const_byte_buffer value,
                                   uint8_t datatype,
                                   uint64_t cas,
                                   Vbid vbucket,
                                   uint64_t bySeqno,
                                   uint64_t revSeqno,
                                   cb::const_byte_buffer meta,
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
     * RAII helper class to update the flowControl object with the number of
     * bytes to free and trigger the consumer notify
     */
    class UpdateFlowControl {
    public:
        UpdateFlowControl(DcpConsumer& consumer, uint32_t bytes)
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
         * If the user no longer wants this instance to perform the update
         * calling release() means this instance will skip the update.
         */
        void release() {
            bytes = 0;
        }

    private:
        DcpConsumer& consumer;
        uint32_t bytes;
    };

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

    uint64_t opaqueCounter;
    size_t processorTaskId;
    std::atomic<enum process_items_error_t> processorTaskState;

    VBReadyQueue vbReady;
    std::atomic<bool> processorNotification;

    std::mutex readyMutex;
    std::list<Vbid> ready;

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
    const std::chrono::seconds dcpNoopTxInterval;

    // Step can't start sending packets until we've received add stream
    bool pendingAddStream = true;

    bool pendingEnableNoop;
    bool pendingSendNoopInterval;
    bool pendingSetPriority;
    bool pendingSupportCursorDropping;
    bool pendingSendStreamEndOnClientStreamClose;
    bool pendingSupportHifiMFU;
    bool pendingEnableExpiryOpcode;

    // Maintains the state of the v7 Dcp Status codes negotiation
    BlockingDcpControlNegotiation v7DcpStatusCodesNegotiation;

    // Flag to state that the DCP consumer has negotiate the with the producer
    // that V7 DCP status codes can be used.
    bool isV7DcpStatusEnabled = false;

    // Maintains the state of the Sync Replication negotiation
    BlockingDcpControlNegotiation syncReplNegotiation;

    // SyncReplication: Producer needs to know the Consumer name to identify
    // the source of received SeqnoAck messages.
    bool pendingSendConsumerName;

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
     *     - producerIsVersion5orHigher=true, if GetErrorMap succeeds
     *     - producerIsVersion5orHigher=false, if GetErrorMap fails
     */
    enum class GetErrorMapState : uint8_t {
        Skip = 0, // Covers "do not send request" and "response ready"
        PendingRequest,
        PendingResponse
    } getErrorMapState;
    bool producerIsVersion5orHigher;

    /**
     * Handles the negotiation for IncludeDeletedUserXattrs.
     * The final purpose is for the Consumer to know if the Producer supports
     * IncludeDeletedUserXattrs, to enforce the proper validation on the payload
     * for normal/sync DCP delete.
     */
    BlockingDcpControlNegotiation deletedUserXattrsNegotiation;

    /* Indicates if the 'Processor' task is running */
    std::atomic<bool> processorTaskRunning;

    FlowControl flowControl;

       /**
     * An upper bound on how many times drainStreamsBufferedItems will
     * call into processBufferedMessages before returning and triggering
     * Processor to yield. Initialised from the configuration
     *  'dcp_consumer_process_buffered_messages_yield_limit'
     */
    size_t processBufferedMessagesYieldThreshold;

    /**
     * An upper bound on how many items a single consumer stream will process
     * in one call of stream->processBufferedMessages()
     */
    size_t processBufferedMessagesBatchSize;

    /**
     * Whether this consumer should just sanitize invalid payloads in deletions
     * or fail the operation if an invalid payload is detected.
     * Non-const as the related configuration param is dynamic.
     */
    std::atomic_bool allowSanitizeValueInDeletion;

    bool alwaysBufferOperations{false};

    static const std::string noopCtrlMsg;
    static const std::string noopIntervalCtrlMsg;
    static const std::string connBufferCtrlMsg;
    static const std::string priorityCtrlMsg;
    static const std::string cursorDroppingCtrlMsg;
    static const std::string sendStreamEndOnClientStreamCloseCtrlMsg;
    static const std::string hifiMFUCtrlMsg;
    static const std::string enableOpcodeExpiryCtrlMsg;
};

/*
 * Task that orchestrates rollback on Consumer,
 * runs in background.
 */
class RollbackTask : public GlobalTask {
public:
    RollbackTask(EventuallyPersistentEngine* e,
                 uint32_t opaque_,
                 Vbid vbid_,
                 uint64_t rollbackSeqno_,
                 std::shared_ptr<DcpConsumer> conn)
        : GlobalTask(e, TaskId::RollbackTask, 0, false),
          description("Running rollback task for " + vbid_.to_string()),
          engine(e),
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
