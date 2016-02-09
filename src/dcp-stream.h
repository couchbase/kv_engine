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

#include <queue>

class EventuallyPersistentEngine;
class MutationResponse;
class SetVBucketState;
class SnapshotMarker;
class DcpResponse;

class DcpConsumer;
typedef SingleThreadedRCPtr<DcpConsumer> dcp_consumer_t;

class DcpProducer;
typedef SingleThreadedRCPtr<DcpProducer> dcp_producer_t;

typedef enum {
    STREAM_PENDING,
    STREAM_BACKFILLING,
    STREAM_IN_MEMORY,
    STREAM_TAKEOVER_SEND,
    STREAM_TAKEOVER_WAIT,
    STREAM_READING,
    STREAM_DEAD
} stream_state_t;

typedef enum {
    //! The stream ended due to all items being streamed
    END_STREAM_OK,
    //! The stream closed early due to a close stream message
    END_STREAM_CLOSED,
    //! The stream closed early because the vbucket state changed
    END_STREAM_STATE,
    //! The stream closed early because the connection was disconnected
    END_STREAM_DISCONNECTED
} end_stream_status_t;

typedef enum {
    STREAM_ACTIVE,
    STREAM_NOTIFIER,
    STREAM_PASSIVE
} stream_type_t;

typedef enum {
    none,
    disk,
    memory
} snapshot_type_t;

typedef enum {
    all_processed,
    more_to_process,
    cannot_process
} process_items_error_t;

class Stream : public RCValue {
public:
    Stream(const std::string &name, uint32_t flags, uint32_t opaque,
           uint16_t vb, uint64_t start_seqno, uint64_t end_seqno,
           uint64_t vb_uuid, uint64_t snap_start_seqno,
           uint64_t snap_end_seqno);

    virtual ~Stream() {}

    uint32_t getFlags() { return flags_; }

    uint16_t getVBucket() { return vb_; }

    uint32_t getOpaque() { return opaque_; }

    uint64_t getStartSeqno() { return start_seqno_; }

    uint64_t getEndSeqno() { return end_seqno_; }

    uint64_t getVBucketUUID() { return vb_uuid_; }

    uint64_t getSnapStartSeqno() { return snap_start_seqno_; }

    uint64_t getSnapEndSeqno() { return snap_end_seqno_; }

    stream_state_t getState() { return state_; }

    stream_type_t getType() { return type_; }

    virtual void addStats(ADD_STAT add_stat, const void *c);

    virtual DcpResponse* next() = 0;

    virtual uint32_t setDead(end_stream_status_t status) = 0;

    virtual void notifySeqnoAvailable(uint64_t seqno) {}

    bool isActive() {
        return state_ != STREAM_DEAD;
    }

    void clear() {
        LockHolder lh(streamMutex);
        clear_UNLOCKED();
    }

protected:

    const char* stateName(stream_state_t st) const;

    void clear_UNLOCKED();

    /* To be called after getting streamMutex lock */
    void pushToReadyQ(DcpResponse* resp);

    /* To be called after getting streamMutex lock */
    void popFromReadyQ(void);

    uint64_t getReadyQueueMemory(void);

    const std::string &name_;
    uint32_t flags_;
    uint32_t opaque_;
    uint16_t vb_;
    uint64_t start_seqno_;
    uint64_t end_seqno_;
    uint64_t vb_uuid_;
    uint64_t snap_start_seqno_;
    uint64_t snap_end_seqno_;
    stream_state_t state_;
    stream_type_t type_;

    AtomicValue<bool> itemsReady;
    Mutex streamMutex;
    std::queue<DcpResponse*> readyQ;

    const static uint64_t dcpMaxSeqno;

private:
    /* This tracks the memory occupied by elements in the readyQ */
    uint64_t readyQueueMemory;
};

typedef RCPtr<Stream> stream_t;

class ActiveStreamCheckpointProcessorTask;

class ActiveStream : public Stream {
public:
    ActiveStream(EventuallyPersistentEngine* e, dcp_producer_t p,
                 const std::string &name, uint32_t flags, uint32_t opaque,
                 uint16_t vb, uint64_t st_seqno, uint64_t en_seqno,
                 uint64_t vb_uuid, uint64_t snap_start_seqno,
                 uint64_t snap_end_seqno, ExTask task);

    ~ActiveStream() {
        LockHolder lh(streamMutex);
        transitionState(STREAM_DEAD);
        clear_UNLOCKED();
    }

    DcpResponse* next();

    void setActive() {
        LockHolder lh(streamMutex);
        if (state_ == STREAM_PENDING) {
            transitionState(STREAM_BACKFILLING);
        }
    }

    uint32_t setDead(end_stream_status_t status);

    void notifySeqnoAvailable(uint64_t seqno);

    void snapshotMarkerAckReceived();

    void setVBucketStateAckRecieved();

    void incrBackfillRemaining(size_t by) {
        backfillRemaining += by;
    }

    void markDiskSnapshot(uint64_t startSeqno, uint64_t endSeqno);

    void backfillReceived(Item* itm);

    void completeBackfill();

    void addStats(ADD_STAT add_stat, const void *c);

    void addTakeoverStats(ADD_STAT add_stat, const void *c);

    size_t getItemsRemaining();

    const char* logHeader();

    // Runs on ActiveStreamCheckpointProcessorTask
    void nextCheckpointItemTask();

protected:
    // Returns the outstanding items for the stream's checkpoint cursor.
    void getOutstandingItems(RCPtr<VBucket> &vb, std::deque<queued_item> &items);

    // Given a set of queued items, create mutation responses for each item,
    // and pass onto the producer associated with this stream.
    void processItems(std::deque<queued_item>& items);

    bool nextCheckpointItem();

private:

    void transitionState(stream_state_t newState);

    DcpResponse* backfillPhase();

    DcpResponse* inMemoryPhase();

    DcpResponse* takeoverSendPhase();

    DcpResponse* takeoverWaitPhase();

    DcpResponse* deadPhase();

    DcpResponse* nextQueuedItem();

    void snapshot(std::deque<MutationResponse*>& snapshot, bool mark);

    void endStream(end_stream_status_t reason);

    void scheduleBackfill();

    const char* getEndStreamStatusStr(end_stream_status_t status);

    bool isCurrentSnapshotCompleted() const;

    //! The last sequence number queued from disk or memory
    uint64_t lastReadSeqno;
    //! The last sequence number sent to the network layer
    uint64_t lastSentSeqno;
    //! The last known seqno pointed to by the checkpoint cursor
    uint64_t curChkSeqno;
    //! The current vbucket state to send in the takeover stream
    vbucket_state_t takeoverState;
    //! The amount of items remaining to be read from disk
    size_t backfillRemaining;

    //! The amount of items that have been read from disk
    size_t itemsFromBackfill;
    //! The amount of items that have been read from memory
    size_t itemsFromMemory;
    //! Whether ot not this is the first snapshot marker sent
    bool firstMarkerSent;

    AtomicValue<int> waitForSnapshot;

    EventuallyPersistentEngine* engine;
    dcp_producer_t producer;
    AtomicValue<bool> isBackfillTaskRunning;

    //! Last snapshot end seqno sent to the DCP client
    uint64_t lastSentSnapEndSeqno;

    ExTask checkpointCreatorTask;

    /* Flag used by checkpointCreatorTask that is set before all items are
       extracted for given checkpoint cursor, and is unset after all retrieved
       items are added to the readyQ */
    AtomicValue<bool> chkptItemsExtractionInProgress;

};


class ActiveStreamCheckpointProcessorTask : public GlobalTask {
public:
    ActiveStreamCheckpointProcessorTask(EventuallyPersistentEngine& e)
      : GlobalTask(&e, Priority::ActiveStreamCheckpointProcessor, INT_MAX, false),
      notified(false),
      iterationsBeforeYield(e.getConfiguration()
                            .getDcpProducerSnapshotMarkerYieldLimit()) { }

    std::string getDescription() {
        std::string rv("Process checkpoint(s) for DCP producer");
        return rv;
    }

    bool run();
    void schedule(stream_t stream);
    void wakeup();

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

    void pushUnique(stream_t stream) {
        LockHolder lh(workQueueLock);
        if (queuedVbuckets.count(stream->getVBucket()) == 0) {
            queue.push(stream);
            queuedVbuckets.insert(stream->getVBucket());
        }
    }

    Mutex workQueueLock;

    /**
     * Maintain a queue of unique stream_t
     * There's no need to have the same stream in the queue more than once
     */
    std::queue<stream_t> queue;
    std::set<uint16_t> queuedVbuckets;

    AtomicValue<bool> notified;
    size_t iterationsBeforeYield;
};

class NotifierStream : public Stream {
public:
    NotifierStream(EventuallyPersistentEngine* e, dcp_producer_t producer,
                   const std::string &name, uint32_t flags, uint32_t opaque,
                   uint16_t vb, uint64_t start_seqno, uint64_t end_seqno,
                   uint64_t vb_uuid, uint64_t snap_start_seqno,
                   uint64_t snap_end_seqno);

    ~NotifierStream() {
        LockHolder lh(streamMutex);
        transitionState(STREAM_DEAD);
        clear_UNLOCKED();
    }

    DcpResponse* next();

    uint32_t setDead(end_stream_status_t status);

    void notifySeqnoAvailable(uint64_t seqno);

private:

    void transitionState(stream_state_t newState);

    dcp_producer_t producer;
};

class PassiveStream : public Stream {
public:
    PassiveStream(EventuallyPersistentEngine* e, dcp_consumer_t consumer,
                  const std::string &name, uint32_t flags, uint32_t opaque,
                  uint16_t vb, uint64_t start_seqno, uint64_t end_seqno,
                  uint64_t vb_uuid, uint64_t snap_start_seqno,
                  uint64_t snap_end_seqno, uint64_t vb_high_seqno);

    ~PassiveStream();

    process_items_error_t processBufferedMessages(uint32_t &processed_bytes);

    DcpResponse* next();

    uint32_t setDead(end_stream_status_t status);

    void acceptStream(uint16_t status, uint32_t add_opaque);

    void reconnectStream(RCPtr<VBucket> &vb, uint32_t new_opaque,
                         uint64_t start_seqno);

    ENGINE_ERROR_CODE messageReceived(DcpResponse* response);

    void addStats(ADD_STAT add_stat, const void *c);

    static const size_t batchSize;

private:

    ENGINE_ERROR_CODE processMutation(MutationResponse* mutation);

    ENGINE_ERROR_CODE commitMutation(MutationResponse* mutation,
                                     bool backfillPhase);

    ENGINE_ERROR_CODE processDeletion(MutationResponse* deletion);

    ENGINE_ERROR_CODE commitDeletion(MutationResponse* deletion,
                                     bool backfillPhase);

    void handleSnapshotEnd(RCPtr<VBucket>& vb, uint64_t byseqno);

    void processMarker(SnapshotMarker* marker);

    void processSetVBucketState(SetVBucketState* state);

    void transitionState(stream_state_t newState);

    uint32_t clearBuffer();

    const char* getEndStreamStatusStr(end_stream_status_t status);

    EventuallyPersistentEngine* engine;
    dcp_consumer_t consumer;
    uint64_t last_seqno;

    uint64_t cur_snapshot_start;
    uint64_t cur_snapshot_end;
    snapshot_type_t cur_snapshot_type;
    bool cur_snapshot_ack;
    bool saveSnapshot;

    struct Buffer {
        Buffer() : bytes(0), items(0) {}
        size_t bytes;
        size_t items;
        Mutex bufMutex;
        std::queue<DcpResponse*> messages;
    } buffer;
};

typedef RCPtr<PassiveStream> passive_stream_t;

#endif  // SRC_DCP_STREAM_H_
