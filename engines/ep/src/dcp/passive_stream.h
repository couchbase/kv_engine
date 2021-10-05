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
#include "spdlog/common.h"
#include "utilities/testing_hook.h"
#include "vbucket_fwd.h"

#include <engines/ep/src/collections/collections_types.h>
#include <memcached/engine_error.h>

class AbortSyncWriteConsumer;
class BucketLogger;
class CommitSyncWriteConsumer;
class CreateCollectionEvent;
class CreateScopeEvent;
class DropCollectionEvent;
class DropScopeEvent;
class EventuallyPersistentEngine;
class MutationConsumerMessage;
class SystemEventMessage;

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

    ~PassiveStream() override;

    /**
     * Process upto batchSize buffered items - trying to set/delete etc...
     *
     * @param batchSize the maximum number of items to process
     * @return pair of status and count of unprocessed items
     */
    std::pair<process_items_error_t, size_t> processBufferedMessages(
            size_t batchSize);

    std::unique_ptr<DcpResponse> next();

    uint32_t setDead(cb::mcbp::DcpStreamEndStatus status) override;

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
     * @params response The dcp message that needs to be processed.
     * @returns the error code from processing the message.
     */
    virtual cb::engine_errc messageReceived(
            std::unique_ptr<DcpResponse> response);

    void addStats(const AddStatFn& add_stat, const CookieIface* c) override;

    /**
     * Push a SeqnoAck message over this stream.
     *
     * @param seqno The payload
     */
    void seqnoAck(int64_t seqno);

    static std::string to_string(StreamState st);

    size_t getNumBufferItems() const {
        std::lock_guard<std::mutex> lh(buffer.bufMutex);
        return buffer.messages.size();
    }

protected:
    bool transitionState(StreamState newState);

    /**
     * An enum specifically for passing the type of message that is to be
     * processed inside processMessage
     */
    enum MessageType : uint8_t {
        Mutation,
        Deletion,
        Expiration,
        Prepare,
    };

    /**
     * processMessage is a wrapper function containing the common elements for
     * dealing with incoming mutation messages. This also deals with the
     * differences between processing a mutation and deletion/expiration.
     *
     * @param message The message sent to the DcpConsumer/PassiveStream
     * @param messageType The type of message to process (see MessageType enum)
     */
    cb::engine_errc processMessage(MutationConsumerMessage* message,
                                   MessageType messageType);
    /**
     * Deal with incoming mutation sent to the DcpConsumer/PassiveStream by
     * passing to processMessage with MessageType::Mutation
     */
    virtual cb::engine_errc processMutation(MutationConsumerMessage* mutation);
    /**
     * Deal with incoming deletion sent to the DcpConsumer/PassiveStream by
     * passing to processMessage with MessageType::Deletion
     */
    cb::engine_errc processDeletion(MutationConsumerMessage* deletion);

    /**
     * Deal with incoming expiration sent to the DcpConsumer/PassiveStream by
     * passing to processMessage with MessageType::Expiration
     */
    cb::engine_errc processExpiration(MutationConsumerMessage* expiration);

    /// Process an incoming prepare.
    cb::engine_errc processPrepare(MutationConsumerMessage* expiration);

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
     * Process a create collection event, creating the collection on vb
     *
     * @param vb Vbucket onto which the collection is created.
     * @param event The collection system event creating the collection.
     */
    cb::engine_errc processCreateCollection(VBucket& vb,
                                            const CreateCollectionEvent& event);

    /**
     * Process a begin delete collection event.
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

    void handleSnapshotEnd(VBucketPtr& vb, uint64_t byseqno);

    virtual void processMarker(SnapshotMarker* marker);

    void processSetVBucketState(SetVBucketState* state);

    uint32_t clearBuffer_UNLOCKED();

    /**
     * Push a StreamRequest into the readyQueue. The StreamRequest is initiaised
     * from the object's state except for the uuid.
     * This function assumes the caller is holding streamMutex.
     *
     * @params vb_uuid The VB UUID to use in the StreamRequest.
     */
    void streamRequest_UNLOCKED(uint64_t vb_uuid);

    template <typename... Args>
    void log(spdlog::level::level_enum severity,
             const char* fmt,
             Args... args) const;

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

    const std::string createStreamReqValue() const;

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

    std::atomic<uint64_t> last_seqno;

    std::atomic<uint64_t> cur_snapshot_start;
    std::atomic<uint64_t> cur_snapshot_end;
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

    // To keep the collections manifest for the Replica consistent we cannot
    // allow it to stream from an Active that is behind in terms of the
    // collections manifest. Send the collections manifest uid to the Active
    // which will decide if it can stream data to us.
    const Collections::ManifestUid vb_manifest_uid;

    struct Buffer {
        Buffer();

        ~Buffer();

        bool empty() const;

        void push(std::unique_ptr<DcpResponse> message);

        /*
         * Caller must of locked bufMutex and pass as lh (not asserted)
         * @param lh caller must lock bufMutex and pass reference to lock holder
         * @param bytesPopped how many 'bytes' were popped
         * @return size of the queue after doing the pop
         */
        size_t pop_front(std::unique_lock<std::mutex>& lh, size_t bytesPopped);

        /*
         * Return a reference to the item at the front.
         * The user must pass a lock to bufMutex.
         */
        std::unique_ptr<DcpResponse>& front(std::unique_lock<std::mutex>& lh);

        size_t bytes;
        /* Lock ordering w.r.t to streamMutex:
           First acquire bufMutex and then streamMutex */
        mutable std::mutex bufMutex;
        std::deque<std::unique_ptr<DcpResponse> > messages;
    } buffer;

    /*
     * MB-31410: Only used for testing.
     * This hook is executed in the PassiveStream::processBufferedMessages
     * function, just after we have got the front message from the buffer.
     * Used for triggering an error condition where the front-end may process
     * new incoming messages before the DcpConsumerTask has processed all
     * messages in the buffer.
     */
    TestingHook<> processBufferedMessages_postFront_Hook;

    // Flag indicating if the most recent call to processMessage
    // backed off due to ENOMEM. Only used for limiting logging
    std::atomic<bool> isNoMemory{false};

    bool alwaysBufferOperations{false};
};
