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

#include "ep_engine.h"
#include "ext_meta_parser.h"
#include "dcp/dcp-types.h"
#include "dcp/producer.h"
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

namespace Collections {
namespace VB {
class Filter;
}
}

enum end_stream_status_t {
    //! The stream ended due to all items being streamed
    END_STREAM_OK,
    //! The stream closed early due to a close stream message
    END_STREAM_CLOSED,
    //! The stream closed early because the vbucket state changed
    END_STREAM_STATE,
    //! The stream closed early because the connection was disconnected
    END_STREAM_DISCONNECTED,
    //! The stream was closed early because it was too slow (currently unused,
    //! but not deleted because it is part of the externally-visible API)
    END_STREAM_SLOW,
    //! The stream closed early due to backfill failure
    END_STREAM_BACKFILL_FAIL
};

enum backfill_source_t {
    BACKFILL_FROM_MEMORY,
    BACKFILL_FROM_DISK
};

class Stream : public RCValue {
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

    virtual DcpResponse* next() = 0;

    virtual uint32_t setDead(end_stream_status_t status) = 0;

    virtual void notifySeqnoAvailable(uint64_t seqno) {}

    void addFilter(const Collections::Filter& filter,
                   const Collections::VB::Manifest& manifest);

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
    void pushToReadyQ(DcpResponse* resp);

    /* To be called after getting streamMutex lock */
    void popFromReadyQ(void);

    uint64_t getReadyQueueMemory(void);

    /**
     * Sub-function of pushToReadyQ.
     * @return true if the DcpRepsonse should be queued, false to skip
     */
    virtual bool queueResponse(DcpResponse* resp) const {
        return true;
    }

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
    std::queue<DcpResponse*> readyQ;

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

class ActiveStreamCheckpointProcessorTask;

class ActiveStream : public Stream {
public:
    ActiveStream(EventuallyPersistentEngine* e,
                 dcp_producer_t p,
                 const std::string& name,
                 uint32_t flags,
                 uint32_t opaque,
                 uint16_t vb,
                 uint64_t st_seqno,
                 uint64_t en_seqno,
                 uint64_t vb_uuid,
                 uint64_t snap_start_seqno,
                 uint64_t snap_end_seqno,
                 IncludeValue includeVal,
                 IncludeXattrs includeXattrs,
                 std::unique_ptr<Collections::VB::Filter> filter);

    virtual ~ActiveStream();

    DcpResponse* next();

    void setActive() {
        LockHolder lh(streamMutex);
        if (isPending()) {
            transitionState(StreamState::Backfilling);
        }
    }

    uint32_t setDead(end_stream_status_t status);

    void notifySeqnoAvailable(uint64_t seqno);

    void snapshotMarkerAckReceived();

    void setVBucketStateAckRecieved();

    void incrBackfillRemaining(size_t by) {
        backfillRemaining.fetch_add(by, std::memory_order_relaxed);
    }

    void markDiskSnapshot(uint64_t startSeqno, uint64_t endSeqno);

    bool backfillReceived(std::unique_ptr<Item> itm,
                          backfill_source_t backfill_source,
                          bool force);

    void completeBackfill();

    bool isCompressionEnabled();

    void addStats(ADD_STAT add_stat, const void *c);

    void addTakeoverStats(ADD_STAT add_stat, const void *c, const VBucket& vb);

    /* Returns a count of how many items are outstanding to be sent for this
     * stream's vBucket.
     */
    size_t getItemsRemaining();

    uint64_t getLastReadSeqno() const;

    uint64_t getLastSentSeqno() const;

    const Logger& getLogger() const;

    // Runs on ActiveStreamCheckpointProcessorTask
    void nextCheckpointItemTask();

    /* Function to handle a slow stream that is supposedly hogging memory in
       checkpoint mgr. Currently we handle the slow stream by switching from
       in-memory to backfilling */
    void handleSlowStream();

    /// @return true if both includeValue and includeXattributes are set to No,
    /// otherwise return false.
    bool isKeyOnly() const {
        return (includeValue == IncludeValue::No) &&
               (includeXattributes == IncludeXattrs::No);
    }

    /// @returns a copy of the current collections separator.
    std::string getCurrentSeparator() const {
        return currentSeparator;
    }

protected:
    // Returns the outstanding items for the stream's checkpoint cursor.
    void getOutstandingItems(VBucketPtr &vb, std::vector<queued_item> &items);

    // Given a set of queued items, create mutation responses for each item,
    // and pass onto the producer associated with this stream.
    void processItems(std::vector<queued_item>& items);

    bool nextCheckpointItem();

    DcpResponse* nextQueuedItem();

    /**
     * @return a DcpResponse to represent the item. This will be either a
     *         MutationResponse or SystemEventProducerMessage.
     */
    std::unique_ptr<DcpResponse> makeResponseFromItem(queued_item& item);

    /* The transitionState function is protected (as opposed to private) for
     * testing purposes.
     */
    void transitionState(StreamState newState);

    /**
     * Check to see if the response is a SystemEvent and if so, apply any
     * actions to the stream.
     *
     * @param response A DcpResponse that is about to be sent to a client
     */
    void processSystemEvent(DcpResponse* response);

    /**
     * Sub-function of Stream::pushToReadyQ.
     *
     * @return true if the DcpRepsonse should be queued, false to skip
     */
    bool queueResponse(DcpResponse* resp) const;

    /**
     * Registers a cursor with a given CheckpointManager.
     *
     * @param chkptmgr  The CheckpointManager the cursor will be registered to.
     * @param startBySeqno  The bySeqno from where to start retrieving items.
     */
    virtual void registerCursor(CheckpointManager& chkptmgr,
                                uint64_t startBySeqno);


    /* Indicates that a backfill has been scheduled and has not yet completed.
     * Is protected (as opposed to private) for testing purposes.
     */
    std::atomic<bool> isBackfillTaskRunning;

    /* Indicates if another backfill must be scheduled following the completion
     * of current running backfill.  Guarded by streamMutex.
     * Is protected (as opposed to private) for testing purposes.
     */
    bool pendingBackfill;

    //! Stats to track items read and sent from the backfill phase
    struct {
        std::atomic<size_t> memory;
        std::atomic<size_t> disk;
        std::atomic<size_t> sent;
    } backfillItems;

    /* The last sequence number queued from disk or memory and is
       snapshotted and put onto readyQ */
    std::atomic<uint64_t> lastReadSeqno;

    /* backfillRemaining is a stat recording the amount of
     * items remaining to be read from disk.  It is an atomic
     * because otherwise the function incrBackfillRemaining
     * must acquire the streamMutex lock.
     */
    std::atomic<size_t> backfillRemaining;

    DcpResponse* backfillPhase(std::lock_guard<std::mutex>& lh);

private:

    DcpResponse* next(std::lock_guard<std::mutex>& lh);

    DcpResponse* inMemoryPhase();

    DcpResponse* takeoverSendPhase();

    DcpResponse* takeoverWaitPhase();

    DcpResponse* deadPhase();

    void snapshot(std::deque<DcpResponse*>& snapshot, bool mark);

    void endStream(end_stream_status_t reason);

    /* reschedule = FALSE ==> First backfill on the stream
     * reschedule = TRUE ==> Schedules another backfill on the stream that has
     *                       finished backfilling once and still in
     *                       STREAM_BACKFILLING state or in STREAM_IN_MEMORY
     *                       state.
     * Note: Expects the streamMutex to be acquired when called
     */
    void scheduleBackfill_UNLOCKED(bool reschedule);

    const char* getEndStreamStatusStr(end_stream_status_t status);

    bool isCurrentSnapshotCompleted() const;

    /* Drop the cursor registered with the checkpoint manager.
     * Note: Expects the streamMutex to be acquired when called
     */
    void dropCheckpointCursor_UNLOCKED();

    /* The last sequence number queued from disk or memory, but is yet to be
       snapshotted and put onto readyQ */
    std::atomic<uint64_t> lastReadSeqnoUnSnapshotted;

    //! The last sequence number sent to the network layer
    std::atomic<uint64_t> lastSentSeqno;

    //! The last known seqno pointed to by the checkpoint cursor
    std::atomic<uint64_t> curChkSeqno;

    //! The current vbucket state to send in the takeover stream
    vbucket_state_t takeoverState;

    //! The amount of items that have been sent during the memory phase
    std::atomic<size_t> itemsFromMemoryPhase;

    //! Whether or not this is the first snapshot marker sent
    bool firstMarkerSent;

    std::atomic<int> waitForSnapshot;

    EventuallyPersistentEngine* engine;
    dcp_producer_t producer;

    struct {
        std::atomic<size_t> bytes;
        std::atomic<size_t> items;
    } bufferedBackfill;

    std::atomic<rel_time_t> takeoverStart;
    size_t takeoverSendMaxTime;

    //! Last snapshot end seqno sent to the DCP client
    std::atomic<uint64_t> lastSentSnapEndSeqno;

    /* Flag used by checkpointCreatorTask that is set before all items are
       extracted for given checkpoint cursor, and is unset after all retrieved
       items are added to the readyQ */
    std::atomic<bool> chkptItemsExtractionInProgress;

    // Whether the responses sent using this stream should contain the value
    IncludeValue includeValue;
    // Whether the responses sent using the stream should contain the xattrs
    // (if any exist)
    IncludeXattrs includeXattributes;

    /**
     * A copy of the collections separator so we can generate MutationResponse
     * instances that embed the collection/document-name data so we can
     * replicate that collection information (as a length).
     *
     * As checkpoints/backfills are processed, we will monitor for
     * CollectionsSeparatorChanged events and update the copy accordingly.
     */
    std::string currentSeparator;

    /**
     * The filter the stream will use to decide which keys should be transmitted
     */
    std::unique_ptr<Collections::VB::Filter> filter;
};


class ActiveStreamCheckpointProcessorTask : public GlobalTask {
public:
    ActiveStreamCheckpointProcessorTask(EventuallyPersistentEngine& e)
        : GlobalTask(&e, TaskId::ActiveStreamCheckpointProcessorTask,
                     INT_MAX, false),
      notified(false),
      iterationsBeforeYield(e.getConfiguration()
                            .getDcpProducerSnapshotMarkerYieldLimit()) { }

    cb::const_char_buffer getDescription() {
        return "Process checkpoint(s) for DCP producer";
    }

    bool run();
    void schedule(const stream_t& stream);
    void wakeup();
    void clearQueues();
    size_t queueSize() {
        LockHolder lh(workQueueLock);
        return queue.size();
    }

private:

    stream_t queuePop() {
        stream_t rval;
        LockHolder lh(workQueueLock);
        if (!queue.empty()) {
            rval = queue.front();
            queue.pop();
            queuedVbuckets.erase(rval->getVBucket());
        }
        return rval;
    }

    bool queueEmpty() {
        LockHolder lh(workQueueLock);
        return queue.empty();
    }

    void pushUnique(const stream_t& stream) {
        LockHolder lh(workQueueLock);
        if (queuedVbuckets.count(stream->getVBucket()) == 0) {
            queue.push(stream);
            queuedVbuckets.insert(stream->getVBucket());
        }
    }

    std::mutex workQueueLock;

    /**
     * Maintain a queue of unique stream_t
     * There's no need to have the same stream in the queue more than once
     */
    std::queue<stream_t> queue;
    std::set<uint16_t> queuedVbuckets;

    std::atomic<bool> notified;
    size_t iterationsBeforeYield;
};

class NotifierStream : public Stream {
public:
    NotifierStream(EventuallyPersistentEngine* e,
                   dcp_producer_t producer,
                   const std::string& name,
                   uint32_t flags,
                   uint32_t opaque,
                   uint16_t vb,
                   uint64_t start_seqno,
                   uint64_t end_seqno,
                   uint64_t vb_uuid,
                   uint64_t snap_start_seqno,
                   uint64_t snap_end_seqno,
                   std::unique_ptr<Collections::VB::Filter> filter);

    ~NotifierStream();

    DcpResponse* next();

    uint32_t setDead(end_stream_status_t status);

    void notifySeqnoAvailable(uint64_t seqno);

    void addStats(ADD_STAT add_stat, const void* c);

private:
    /**
     * Sub-function of Stream::pushToReadyQ.
     * @return true if the DcpRepsonse should be queued, false to skip
     */
    bool queueResponse(DcpResponse* resp) const;

    void transitionState(StreamState newState);

    dcp_producer_t producer;

    std::unique_ptr<Collections::VB::Filter> filter;
};

class PassiveStream : public Stream {
public:
    PassiveStream(EventuallyPersistentEngine* e, dcp_consumer_t consumer,
                  const std::string &name, uint32_t flags, uint32_t opaque,
                  uint16_t vb, uint64_t start_seqno, uint64_t end_seqno,
                  uint64_t vb_uuid, uint64_t snap_start_seqno,
                  uint64_t snap_end_seqno, uint64_t vb_high_seqno);

    virtual ~PassiveStream();

    process_items_error_t processBufferedMessages(uint32_t &processed_bytes,
                                                  size_t batchSize);

    DcpResponse* next();

    uint32_t setDead(end_stream_status_t status);

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

    void addStats(ADD_STAT add_stat, const void *c);

    static const size_t batchSize;

protected:

    bool transitionState(StreamState newState);

    ENGINE_ERROR_CODE processMutation(MutationResponse* mutation);

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
    ENGINE_ERROR_CODE processCreateCollection(VBucket& vb,
                                              const CollectionsEvent& event);

    /**
     * Process a begin delete collection event.
     *
     * @param vb Vbucket which we apply the delete on.
     * @param event The collection system event deleting the collection.
     */
    ENGINE_ERROR_CODE processBeginDeleteCollection(
            VBucket& vb, const CollectionsEvent& event);

    /**
     * Process a collections change separator event.
     *
     * @param vb Vbucket which we apply the delete on.
     * @param event The collection system event changing the separator.
     */
    ENGINE_ERROR_CODE processSeparatorChanged(VBucket& vb,
                                              const CollectionsEvent& event);

    void handleSnapshotEnd(VBucketPtr& vb, uint64_t byseqno);

    void processMarker(SnapshotMarker* marker);

    void processSetVBucketState(SetVBucketState* state);

    uint32_t clearBuffer_UNLOCKED();

    const char* getEndStreamStatusStr(end_stream_status_t status);

    /**
     * Push a StreamRequest into the readyQueue. The StreamRequest is initiaised
     * from the object's state except for the uuid.
     * This function assumes the caller is holding streamMutex.
     *
     * @params vb_uuid The VB UUID to use in the StreamRequest.
     */
    void streamRequest_UNLOCKED(uint64_t vb_uuid);

    EventuallyPersistentEngine* engine;
    dcp_consumer_t consumer;

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
