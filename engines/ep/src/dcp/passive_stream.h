/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2018-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#pragma once

#include "dcp/stream.h"
#include "permitted_vb_states.h"
#include "utilities/testing_hook.h"
#include "vbucket_fwd.h"

#include <engines/ep/src/collections/collections_types.h>
#include <memcached/engine_error.h>
#include <platform/non_negative_counter.h>

namespace cb::mcbp {
enum class DcpAddStreamFlag : uint32_t;
}

class AbortSyncWriteConsumer;
class BucketLogger;
class CommitSyncWriteConsumer;
class CreateCollectionEvent;
class CreateScopeEvent;
class DropCollectionEvent;
class DropScopeEvent;
class EventuallyPersistentEngine;
class MutationResponse;
class SystemEventMessage;
class SystemEventConsumerMessage;
class UpdateFlowControl;

class PassiveStream : public Stream {
public:
    /// The states this PassiveStream object can be in.
    enum class StreamState {
        Pending,
        Reading,
        Dead
    };

    PassiveStream(EventuallyPersistentEngine* e,
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

    ~PassiveStream() override;

    /**
     * Observes the memory state of the node and triggers buffer-ack of unacked
     * DCP bytes when the system recovers from OOM.
     *
     * @param [out] processed_bytes
     * @return ProcessUnackedBytesResult, see struct definition for details
     */
    ProcessUnackedBytesResult processUnackedBytes(uint32_t& processed_bytes);

    std::unique_ptr<DcpResponse> next();

    void setDead(cb::mcbp::DcpStreamEndStatus status) override;

    std::string getStreamTypeName() const override;

    std::string getStateName() const override;

    /// @returns true if state_ is not Dead
    bool isActive() const override;

    /// @Returns true if state_ is Pending
    bool isPending() const;

    /**
     * Place a StreamRequest message into the readyQueue, requesting a DCP
     * stream for the given UUID.
     *
     * @params vb_uuid The UUID to use in the StreamRequest.
     */
    void streamRequest(uint64_t vb_uuid);

    void acceptStream(cb::mcbp::Status status, uint32_t add_opaque);

    void reconnectStream(VBucketPtr& vb,
                         uint32_t new_opaque,
                         uint64_t start_seqno);

    /*
     * Calls the appropriate function to process the message.
     *
     * @param response The dcp message that needs to be processed.
     * @param ackSize the value to use when DCP acking - this may differ from
     *        DcpResponse::getMessageSize if for example an Item value was
     *        decompressed
     * @returns the error code from processing the message.
     */
    cb::engine_errc messageReceived(std::unique_ptr<DcpResponse> response,
                                    UpdateFlowControl& ackSize);

    void addStats(const AddStatFn& add_stat, CookieIface& c) override;

    /**
     * Push a SeqnoAck message over this stream.
     *
     * @param seqno The payload
     */
    void seqnoAck(int64_t seqno);

    static std::string to_string(StreamState st);

    /**
     * Inform the stream on the latest seqno successfully processed. The
     * function updates the stream's state in the case where that latest seqno
     * is also the snap-end seqno of the snapshot that PassiveStream is
     * currently receiving.
     *
     * @param seqno
     */
    void handleSnapshotEnd(uint64_t seqno);

    /**
     * @return the number of bytes of DCP messages that have been queued into
     *  the Checkpoint but not acked back to the Producer yet. That happens when
     *  the Consumer's node is OOM.
     *  Unacked bytes processing deferred to the DcpConsumerTask.
     */
    size_t getUnackedBytes() const;

protected:
    bool transitionState(StreamState newState);

    /**
     * Wrapper function containing the common elements for dealing with incoming
     * mutation messages. This also deals with the differences between
     * processing a mutation and deletion/expiration.
     *
     * @param message The message sent to the DcpConsumer/PassiveStream
     * @param enforceMemCheck Whether we want to enforce mem conditions on this
     *  processing
     */
    cb::engine_errc processMessageInner(MutationResponse& message,
                                        EnforceMemCheck enforceMemCheck);

    /// Process an incoming commit of a SyncWrite.
    cb::engine_errc processCommit(const CommitSyncWriteConsumer& commit);

    /// Process an incoming abort of a SyncWrite.
    cb::engine_errc processAbort(const AbortSyncWriteConsumer& abort);

    /**
     * Handle DCP system events against this stream.
     *
     * @param event The system-event to process against the stream.
     */
    cb::engine_errc processSystemEvent(const SystemEventMessage& event);

    /**
     * Inner handler for when messages don't use FlatBuffers
     */
    cb::engine_errc processSystemEvent(VBucket& vb,
                                       const SystemEventMessage& event);

    /**
     * Inner handler for when messages do use FlatBuffers
     */
    cb::engine_errc processSystemEventFlatBuffers(
            VBucket& vb, const SystemEventConsumerMessage& event);

    /**
     * Process a begin collection event against the vb
     *
     * @param vb Vbucket to apply the event to
     * @param event The event data for the collection
     */
    cb::engine_errc processBeginCollection(VBucket& vb,
                                           const CreateCollectionEvent& event);

    /**
     * Process a delete collection event.
     *
     * @param vb Vbucket which we apply the delete on.
     * @param event The collection system event deleting the collection.
     */
    cb::engine_errc processDropCollection(VBucket& vb,
                                          const DropCollectionEvent& event);

    /**
     * Process a create scope event, creating the collection on vb
     *
     * @param vb Vbucket onto which the collection is created.
     * @param event The event data for the create
     */
    cb::engine_errc processCreateScope(VBucket& vb,
                                       const CreateScopeEvent& event);

    /**
     * Process a drop scope event
     *
     * @param vb Vbucket which we apply the drop to
     * @param event The event data for the drop
     */
    cb::engine_errc processDropScope(VBucket& vb, const DropScopeEvent& event);

    /**
     * Process a create collection when FlatBuffers support is enabled
     *
     * @param vb Vbucket which we apply the create to
     * @param event The system event to process
     */
    cb::engine_errc processBeginCollection(
            VBucket& vb, const SystemEventConsumerMessage& event);

    /**
     * Process a modify collection when FlatBuffers support is enabled
     *
     * @param vb Vbucket which we apply the modify to
     * @param event The system event to process
     */
    cb::engine_errc processModifyCollection(
            VBucket& vb, const SystemEventConsumerMessage& event);

    /**
     * Process a drop collection when FlatBuffers support is enabled
     *
     * @param vb Vbucket which we apply the drop to
     * @param event The system event to process
     */
    cb::engine_errc processDropCollection(
            VBucket& vb, const SystemEventConsumerMessage& event);

    /**
     * Process a create scope when FlatBuffers support is enabled
     *
     * @param vb Vbucket which we apply the create to
     * @param event The system event to process
     */
    cb::engine_errc processCreateScope(VBucket& vb,
                                       const SystemEventConsumerMessage& event);

    /**
     * Process a drop scope when FlatBuffers support is enabled
     *
     * @param vb Vbucket which we apply the drop to
     * @param event The system event to process
     */
    cb::engine_errc processDropScope(VBucket& vb,
                                     const SystemEventConsumerMessage& event);

    virtual void processMarker(SnapshotMarker* marker);

    void processSetVBucketState(SetVBucketState* state);

    /**
     * Process a cache transfer message (from a DcpCacheTransfer
     * producer stream).
     *
     * @param resp The cache transfer message to process (which is a
     * MutationResponse with different event)
     * @return cb::engine_errc::success if the item was inserted into the cache
     * otherwise a status code for why not (e.g. nmvb for vbucket state changed)
     */
    cb::engine_errc processCacheTransfer(MutationResponse& resp);

    /**
     * Push a StreamRequest into the readyQueue. The StreamRequest is initiaised
     * from the object's state except for the uuid.
     * This function assumes the caller is holding streamMutex.
     *
     * @params vb_uuid The VB UUID to use in the StreamRequest.
     */
    void streamRequest_UNLOCKED(uint64_t vb_uuid);

    void logWithContext(spdlog::level::level_enum severity,
                        std::string_view msg,
                        cb::logger::Json ctx) const override;

    /**
     * Log when the stream backs off or resumes processing items.
     *
     * Determines if the stream has backed off or resumed based on
     * the passed status. Assumes no_memory indicates the stream will
     * backoff, and that success indicates the stream has resumed
     * processing.
     *
     * @param status last error code returned by the engine when processing
     *               a message
     * @param msgType string indicating the type of the last message processed
     *                ("mutation", "deletion", "expiration", "prepare")
     * @param seqno seqno of last message processed
     */
    void maybeLogMemoryState(cb::engine_errc status,
                             const std::string& msgType,
                             int64_t seqno);

    /**
     * Notifies the consumer connection that the stream has items ready to be
     * pick up.
     */
    void notifyStreamReady();

    [[nodiscard]] std::string createStreamReqValue() const;

    /**
     * RAII class. At dtor the logic triggers post-processMessage steps.
     */
    class ProcessMessageResult {
    public:
        ProcessMessageResult(PassiveStream& stream,
                             cb::engine_errc err,
                             std::optional<int64_t> seqno)
            : stream(&stream), err(err), seqno(seqno){};

        ~ProcessMessageResult();

        ProcessMessageResult(const ProcessMessageResult&) = delete;
        ProcessMessageResult& operator=(const ProcessMessageResult&) = delete;

        ProcessMessageResult(ProcessMessageResult&& other) = default;
        ProcessMessageResult& operator=(ProcessMessageResult&& other) = default;

        cb::engine_errc getError() const {
            return err;
        }

    private:
        PassiveStream* stream;
        cb::engine_errc err;
        std::optional<int64_t> seqno;
    };

    /**
     * Wrapper function to forwarding a response type to the proper processing
     * path.
     *
     * @param resp The DcpResponse to be processed
     * @param enforceMemCheck Whether we want to enforce mem conditions on this
     *  processing
     * @return ProcessMessageResult, see struct for details
     */
    ProcessMessageResult processMessage(gsl::not_null<DcpResponse*> resp,
                                        EnforceMemCheck enforceMemCheck);

    /**
     * Process the given message by bypassing memory checks.
     *
     * @param resp The DcpResponse to be processed
     * @return ProcessMessageResult, see struct for details
     */
    ProcessMessageResult forceMessage(DcpResponse& resp);

    // The current state the stream is in.
    // Atomic to allow reads without having to acquire the streamMutex.
    std::atomic<StreamState> state_{StreamState::Pending};

    EventuallyPersistentEngine* const engine;
    const std::weak_ptr<DcpConsumer> consumerPtr;

    /**
     * Has the consumer associated with this stream negotiated Synchronous
     * Replication?
     * This is local copy of Consumer::supportsSyncReplication, to avoid
     * having to lock the consumerPtr weak_ptr on every SnapshotMarker process.
     */
    bool supportsSyncReplication{false};

    struct Labeller {
        std::string getLabel(const char* name) const;
        const PassiveStream& stream;
    };

    ATOMIC_MONOTONIC4(uint64_t, last_seqno, Labeller, ThrowExceptionPolicy);
    ATOMIC_WEAKLY_MONOTONIC3(uint64_t, cur_snapshot_start, Labeller);
    ATOMIC_MONOTONIC4(uint64_t,
                      cur_snapshot_end,
                      Labeller,
                      ThrowExceptionPolicy);
    std::atomic<Snapshot> cur_snapshot_type;
    bool cur_snapshot_ack;

    // For Durability, a Replica has to acknowledge the current High Prepared
    // Seqno to the Active. For meeting the Durability consistency requirements,
    // the HPS must advance at snapshot boundaries.
    // To achieve that, (1) we sign the current snapshot as "containing at least
    // one Prepare mutation" if any (done by setting the cur_snapshot_prepare
    // flag when a Prepare is processed) and (2) we notify the DurabilityMonitor
    // when the snapshot-end mutation is received (if the flag is set).
    // The DM uses the snap-end seqno for implementing the correct move-logic
    // for HPS.
    // Note that we avoid to notify the DM when the snapshot doesn't contain any
    // Prepare for prevent any performance degradation, as that is done in
    // front-end threads.
    std::atomic<bool> cur_snapshot_prepare;

    // Cache the hps received in a snapshot marker. Remains unset when the
    // producer doesn't support sync-replication or if snapshot marker is
    // not v2.2
    OptionalSeqno cur_snapshot_hps;

    OptionalSeqno cur_snapshot_hcs;

    // To keep the collections manifest for the Replica consistent we cannot
    // allow it to stream from an Active that is behind in terms of the
    // collections manifest. Send the collections manifest uid to the Active
    // which will decide if it can stream data to us.
    const Collections::ManifestUid vb_manifest_uid;

    // This accounts received bytes of DCP messages processed in OOM state.
    // Those messages are queued into the Checkpoints but bytes not acked back
    // to the Producer. Bytes acked back to the Producer when the Consumer
    // recovers from OOM.
    cb::AtomicNonNegativeCounter<size_t> unackedBytes;

    /*
     * MB-56675: This test hook is invoked just before the streamEnd path sets
     * the stream to the dead state (only on the unbuffered path)
     */
    TestingHook<> streamDeadHook;

    /**
     * MB-60468: Test hook executed at PassiveStream::processUnackedBytes'
     * prologue.
     */
    TestingHook<> processUnackedBytes_TestHook;

    // Flag indicating if the most recent call to processMessageInner
    // backed off due to ENOMEM. Only used for limiting logging
    std::atomic<bool> isNoMemory{false};

    // True if the consumer/producer enabled FlatBuffers
    bool flatBuffersSystemEventsEnabled{false};

    // Set of states that the vbucket must match for a PassiveStream to attempt
    // processing messages.
    const PermittedVBStates permittedVBStates{vbucket_state_replica,
                                              vbucket_state_pending};
};
