/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2013 Couchbase, Inc
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

#ifndef SRC_DCP_STREAM_H_
#define SRC_DCP_STREAM_H_ 1

#include "config.h"

#include "collections/vbucket_filter.h"
#include "dcp/dcp-types.h"
#include "dcp/producer.h"
#include "ep_engine.h"
#include "ext_meta_parser.h"
#include "response.h"
#include "vbucket.h"

#include <atomic>
#include <climits>
#include <queue>

class EventuallyPersistentEngine;
class MutationResponse;
class SetVBucketState;
class SnapshotMarker;
class DcpResponse;

enum backfill_source_t {
    BACKFILL_FROM_MEMORY,
    BACKFILL_FROM_DISK
};

class Stream {
public:

    enum class Type {
        Active,
        Notifier,
        Passive
    };

    enum class Snapshot {
           None,
           Disk,
           Memory
    };

    Stream(const std::string &name,
           uint32_t flags,
           uint32_t opaque,
           uint16_t vb,
           uint64_t start_seqno,
           uint64_t end_seqno,
           uint64_t vb_uuid,
           uint64_t snap_start_seqno,
           uint64_t snap_end_seqno,
           Type type);

    virtual ~Stream();

    uint32_t getFlags() { return flags_; }

    uint16_t getVBucket() { return vb_; }

    uint32_t getOpaque() { return opaque_; }

    uint64_t getStartSeqno() { return start_seqno_; }

    uint64_t getEndSeqno() { return end_seqno_; }

    uint64_t getVBucketUUID() { return vb_uuid_; }

    uint64_t getSnapStartSeqno() { return snap_start_seqno_; }

    uint64_t getSnapEndSeqno() { return snap_end_seqno_; }

    virtual void addStats(ADD_STAT add_stat, const void *c);

    virtual std::unique_ptr<DcpResponse> next() = 0;

    virtual uint32_t setDead(end_stream_status_t status) = 0;

    virtual void notifySeqnoAvailable(uint64_t seqno) {}

    const std::string& getName() {
        return name_;
    }

    virtual void setActive() {
        // Stream defaults to do nothing
    }

    Type getType() { return type_; }

    /// @returns true if the stream type is Active
    bool isTypeActive() const;

    /// @returns true if state_ is not Dead
    bool isActive() const;

    /// @Returns true if state_ is Backfilling
    bool isBackfilling() const;

    /// @Returns true if state_ is InMemory
    bool isInMemory() const;

    /// @Returns true if state_ is Pending
    bool isPending() const;

    /// @Returns true if state_ is TakeoverSend
    bool isTakeoverSend() const;

    /// @Returns true if state_ is TakeoverWait
    bool isTakeoverWait() const;

    void clear() {
        LockHolder lh(streamMutex);
        clear_UNLOCKED();
    }

protected:

    // The StreamState is protected as it needs to be accessed by sub-classes
    enum class StreamState {
          Pending,
          Backfilling,
          InMemory,
          TakeoverSend,
          TakeoverWait,
          Reading,
          Dead
      };

    static const std::string to_string(Stream::StreamState type);

    StreamState getState() const { return state_; }

    void clear_UNLOCKED();

    /* To be called after getting streamMutex lock */
    void pushToReadyQ(std::unique_ptr<DcpResponse> resp);

    /* To be called after getting streamMutex lock */
    std::unique_ptr<DcpResponse> popFromReadyQ(void);

    uint64_t getReadyQueueMemory(void);

    /**
     * Uses the associated connection logger to log the message if the
     * connection is alive else uses a default logger
     *
     * @param severity Desired logging level
     * @param fmt Format string
     * @param ... Variable list of params as per the fmt
     */
    virtual void log(EXTENSION_LOG_LEVEL severity,
                     const char* fmt,
                     ...) const = 0;

    const std::string &name_;
    uint32_t flags_;
    uint32_t opaque_;
    uint16_t vb_;
    uint64_t start_seqno_;
    uint64_t end_seqno_;
    uint64_t vb_uuid_;
    uint64_t snap_start_seqno_;
    uint64_t snap_end_seqno_;
    std::atomic<StreamState> state_;
    Type type_;

    std::atomic<bool> itemsReady;
    std::mutex streamMutex;

    /**
     * Ordered queue of DcpResponses to be sent on the stream.
     * Elements are added to this queue by reading from disk/memory etc, and
     * are removed when sending over the network to our peer.
     * The readyQ owns the elements in it.
     */
    std::queue<std::unique_ptr<DcpResponse>> readyQ;

    // Number of items in the readyQ that are not meta items. Used for
    // calculating getItemsRemaining(). Atomic so it can be safely read by
    // getItemsRemaining() without acquiring streamMutex.
    std::atomic<size_t> readyQ_non_meta_items;

    const static uint64_t dcpMaxSeqno;

private:
    /* readyQueueMemory tracks the memory occupied by elements
     * in the readyQ.  It is an atomic because otherwise
       getReadyQueueMemory would need to acquire streamMutex.
     */
    std::atomic <uint64_t> readyQueueMemory;
};

const char* to_string(Stream::Snapshot type);
const std::string to_string(Stream::Type type);

class NotifierStream : public Stream {
public:
    NotifierStream(EventuallyPersistentEngine* e,
                   std::shared_ptr<DcpProducer> producer,
                   const std::string& name,
                   uint32_t flags,
                   uint32_t opaque,
                   uint16_t vb,
                   uint64_t start_seqno,
                   uint64_t end_seqno,
                   uint64_t vb_uuid,
                   uint64_t snap_start_seqno,
                   uint64_t snap_end_seqno);

    std::unique_ptr<DcpResponse> next() override;

    uint32_t setDead(end_stream_status_t status) override;

    void notifySeqnoAvailable(uint64_t seqno) override;

    void addStats(ADD_STAT add_stat, const void* c) override;

private:

    void transitionState(StreamState newState);

    void log(EXTENSION_LOG_LEVEL severity, const char* fmt, ...) const override;

    /**
     * Notifies the producer connection that the stream has items ready to be
     * pick up.
     */
    void notifyStreamReady();

    std::weak_ptr<DcpProducer> producerPtr;
};

class PassiveStream : public Stream {
public:
    PassiveStream(EventuallyPersistentEngine* e,
                  std::shared_ptr<DcpConsumer> consumer,
                  const std::string& name,
                  uint32_t flags,
                  uint32_t opaque,
                  uint16_t vb,
                  uint64_t start_seqno,
                  uint64_t end_seqno,
                  uint64_t vb_uuid,
                  uint64_t snap_start_seqno,
                  uint64_t snap_end_seqno,
                  uint64_t vb_high_seqno);

    virtual ~PassiveStream();

    process_items_error_t processBufferedMessages(uint32_t &processed_bytes,
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

    void acceptStream(uint16_t status, uint32_t add_opaque);

    void reconnectStream(VBucketPtr &vb, uint32_t new_opaque,
                         uint64_t start_seqno);

    /*
     * Calls the appropriate function to process the message.
     *
     * @params response The dcp message that needs to be processed.
     * @returns the error code from processing the message.
     */
    virtual ENGINE_ERROR_CODE messageReceived(
            std::unique_ptr<DcpResponse> response);

    void addStats(ADD_STAT add_stat, const void* c) override;

    static const size_t batchSize;

protected:

    bool transitionState(StreamState newState);

    virtual ENGINE_ERROR_CODE processMutation(MutationResponse* mutation);

    ENGINE_ERROR_CODE processDeletion(MutationResponse* deletion);

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
            VBucket& vb, const CreateOrDeleteCollectionEvent& event);

    /**
     * Process a begin delete collection event.
     *
     * @param vb Vbucket which we apply the delete on.
     * @param event The collection system event deleting the collection.
     */
    ENGINE_ERROR_CODE processBeginDeleteCollection(
            VBucket& vb, const CreateOrDeleteCollectionEvent& event);


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

    void log(EXTENSION_LOG_LEVEL severity, const char* fmt, ...) const override;

    /**
     * Notifies the consumer connection that the stream has items ready to be
     * pick up.
     */
    void notifyStreamReady();

    EventuallyPersistentEngine* engine;
    std::weak_ptr<DcpConsumer> consumerPtr;

    std::atomic<uint64_t> last_seqno;

    std::atomic<uint64_t> cur_snapshot_start;
    std::atomic<uint64_t> cur_snapshot_end;
    std::atomic<Snapshot> cur_snapshot_type;
    bool cur_snapshot_ack;

    struct Buffer {
        Buffer() : bytes(0) {}

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
        std::unique_ptr<DcpResponse> pop_front(std::unique_lock<std::mutex>& lh) {
            std::unique_ptr<DcpResponse> rval(std::move(messages.front()));
            messages.pop_front();
            bytes -= rval->getMessageSize();
            return rval;
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
};

#endif  // SRC_DCP_STREAM_H_
