/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2018 Couchbase, Inc
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

#include "dcp/response.h"
#include "dcp/stream.h"
#include "locks.h"
#include "spdlog/common.h"
#include "vbucket_fwd.h"

#include <memcached/engine_error.h>

class BucketLogger;
class ChangeSeparatorCollectionEvent;
class CreateOrDeleteCollectionEvent;
class EventuallyPersistentEngine;
class SystemEventMessage;

class PassiveStream : public Stream {
public:
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

    virtual ~PassiveStream();

    process_items_error_t processBufferedMessages(uint32_t& processed_bytes,
                                                  size_t batchSize);

    std::unique_ptr<DcpResponse> next() override;

    uint32_t setDead(end_stream_status_t status) override;

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
    virtual ENGINE_ERROR_CODE messageReceived(
            std::unique_ptr<DcpResponse> response);

    void addStats(const AddStatFn& add_stat, const void* c) override;

    /**
     * Push a SeqnoAck message over this stream.
     * The memory/disk seqnos in the SeqnoAck payload are respectively the
     * high-seqno and the last-persisted-seqno for VBucket.
     */
    void seqnoAck();

    static const size_t batchSize;

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
    ENGINE_ERROR_CODE processMessage(MutationConsumerMessage* message,
                                     MessageType messageType);
    /**
     * Deal with incoming mutation sent to the DcpConsumer/PassiveStream by
     * passing to processMessage with MessageType::Mutation
     */
    virtual ENGINE_ERROR_CODE processMutation(
            MutationConsumerMessage* mutation);
    /**
     * Deal with incoming deletion sent to the DcpConsumer/PassiveStream by
     * passing to processMessage with MessageType::Deletion
     */
    ENGINE_ERROR_CODE processDeletion(MutationConsumerMessage* deletion);

    /**
     * Deal with incoming expiration sent to the DcpConsumer/PassiveStream by
     * passing to processMessage with MessageType::Expiration
     */
    ENGINE_ERROR_CODE processExpiration(MutationConsumerMessage* expiration);

    /// Process an incoming prepare.
    ENGINE_ERROR_CODE processPrepare(MutationConsumerMessage* expiration);

    /// Process an incoming commit of a SyncWrite.
    ENGINE_ERROR_CODE processCommit(const CommitSyncWrite& commit);

    /**
     * Handle DCP system events against this stream.
     *
     * @param event The system-event to process against the stream.
     */
    ENGINE_ERROR_CODE processSystemEvent(const SystemEventMessage& event);

    /**
     * Process a create collection event, creating the collection on vb
     *
     * @param vb Vbucket onto which the collection is created.
     * @param event The collection system event creating the collection.
     */
    ENGINE_ERROR_CODE processCreateCollection(
            VBucket& vb, const CreateCollectionEvent& event);

    /**
     * Process a begin delete collection event.
     *
     * @param vb Vbucket which we apply the delete on.
     * @param event The collection system event deleting the collection.
     */
    ENGINE_ERROR_CODE processDropCollection(VBucket& vb,
                                            const DropCollectionEvent& event);

    /**
     * Process a create scope event, creating the collection on vb
     *
     * @param vb Vbucket onto which the collection is created.
     * @param event The event data for the create
     */
    ENGINE_ERROR_CODE processCreateScope(VBucket& vb,
                                         const CreateScopeEvent& event);

    /**
     * Process a drop scope event
     *
     * @param vb Vbucket which we apply the drop to
     * @param event The event data for the drop
     */
    ENGINE_ERROR_CODE processDropScope(VBucket& vb,
                                       const DropScopeEvent& event);

    void handleSnapshotEnd(VBucketPtr& vb, uint64_t byseqno);

    virtual void processMarker(SnapshotMarker* marker);

    void processSetVBucketState(SetVBucketState* state);

    uint32_t clearBuffer_UNLOCKED();

    std::string getEndStreamStatusStr(end_stream_status_t status);

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
     * Notifies the consumer connection that the stream has items ready to be
     * pick up.
     */
    void notifyStreamReady();

    const std::string createStreamReqValue() const;

    EventuallyPersistentEngine* engine;
    std::weak_ptr<DcpConsumer> consumerPtr;

    std::atomic<uint64_t> last_seqno;

    std::atomic<uint64_t> cur_snapshot_start;
    std::atomic<uint64_t> cur_snapshot_end;
    std::atomic<Snapshot> cur_snapshot_type;
    bool cur_snapshot_ack;

    // To keep the collections manifest for the Replica consistent we cannot
    // allow it to stream from an Active that is behind in terms of the
    // collections manifest. Send the collections manifest uid to the Active
    // which will decide if it can stream data to us.
    const Collections::ManifestUid vb_manifest_uid;

    struct Buffer {
        Buffer() : bytes(0) {
        }

        bool empty() const {
            LockHolder lh(bufMutex);
            return messages.empty();
        }

        void push(std::unique_ptr<DcpResponse> message) {
            std::lock_guard<std::mutex> lg(bufMutex);
            bytes += message->getMessageSize();
            messages.push_back(std::move(message));
        }

        /*
         * Caller must of locked bufMutex and pass as lh (not asserted)
         */
        std::unique_ptr<DcpResponse> pop_front(
                std::unique_lock<std::mutex>& lh) {
            std::unique_ptr<DcpResponse> rval(std::move(messages.front()));
            messages.pop_front();
            bytes -= rval->getMessageSize();
            return rval;
        }

        /*
         * Return a reference to the item at the front.
         * The user must pass a lock to bufMutex.
         */
        std::unique_ptr<DcpResponse>& front(std::unique_lock<std::mutex>& lh) {
            return messages.front();
        }

        /*
         * Caller must of locked bufMutex and pass as lh (not asserted)
         */
        void push_front(std::unique_ptr<DcpResponse> message,
                        std::unique_lock<std::mutex>& lh) {
            bytes += message->getMessageSize();
            messages.push_front(std::move(message));
        }

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
    std::function<void()> processBufferedMessages_postFront_Hook;
};
