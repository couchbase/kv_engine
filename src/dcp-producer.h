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

#ifndef SRC_DCP_PRODUCER_H_
#define SRC_DCP_PRODUCER_H_ 1

#include "config.h"

#include "tapconnection.h"
#include "dcp-stream.h"

class DcpResponse;

class DcpProducer : public Producer {
public:

    DcpProducer(EventuallyPersistentEngine &e, const void *cookie,
                const std::string &n, bool notifyOnly);

    ENGINE_ERROR_CODE streamRequest(uint32_t flags, uint32_t opaque,
                                    uint16_t vbucket, uint64_t start_seqno,
                                    uint64_t end_seqno, uint64_t vbucket_uuid,
                                    uint64_t last_seqno, uint64_t next_seqno,
                                    uint64_t *rollback_seqno,
                                    dcp_add_failover_log callback);

    ENGINE_ERROR_CODE getFailoverLog(uint32_t opaque, uint16_t vbucket,
                                     dcp_add_failover_log callback);

    ENGINE_ERROR_CODE step(struct dcp_message_producers* producers);

    ENGINE_ERROR_CODE bufferAcknowledgement(uint32_t opaque, uint16_t vbucket,
                                            uint32_t buffer_bytes);

    ENGINE_ERROR_CODE control(uint32_t opaque, const void* key, uint16_t nkey,
                              const void* value, uint32_t nvalue);

    ENGINE_ERROR_CODE handleResponse(protocol_binary_response_header *resp);

    void addStats(ADD_STAT add_stat, const void *c);

    void addTakeoverStats(ADD_STAT add_stat, const void* c, uint16_t vbid);

    void aggregateQueueStats(ConnCounter* aggregator);

    void setDisconnect(bool disconnect);

    void notifySeqnoAvailable(uint16_t vbucket, uint64_t seqno);

    void vbucketStateChanged(uint16_t vbucket, vbucket_state_t state);

    void closeAllStreams();

    const char *getType() const;

    bool isTimeForNoop();

    void setTimeForNoop();

    void clearQueues();

    void appendQueue(std::list<queued_item> *q);

    size_t getBackfillQueueSize();

    size_t getItemsSent();

    size_t getTotalBytes();

    bool windowIsFull();

    void flush();

    std::list<uint16_t> getVBList(void);

    /**
     * Close the stream for given vbucket stream
     *
     * @param vbucket the if for the vbucket to close
     * @return ENGINE_SUCCESS upon a successful close
     *         ENGINE_NOT_MY_VBUCKET the vbucket stream doesn't exist
     */
    ENGINE_ERROR_CODE closeStream(uint32_t opaque, uint16_t vbucket);

    void notifyStreamReady(uint16_t vbucket, bool schedule);

    void notifyPaused(bool schedule);

    class BufferLog {
    public:

        /*
            BufferLog has 3 states.
            Disabled - Flow-control is not in-use.
             This is indicated by setting the size to 0 (i.e. setBufferSize(0)).

            SpaceAvailable - There is *some* space available. You can always
             insert n-bytes even if there's n-1 bytes spare.

            Full - inserts have taken the number of bytes available equal or
             over the buffer size.
        */
        enum State {
            Disabled,
            Full,
            SpaceAvailable
        };

        BufferLog(DcpProducer& p)
            : producer(p), maxBytes(0), bytesSent(0), ackedBytes(0) {}

        void setBufferSize(size_t maxBytes);

        void addStats(ADD_STAT add_stat, const void *c);

        /*
            Return false if the log is full.

            Returns true if the bytes fit or if the buffer log is disabled.
              The tracked bytes is increased.
        */
        bool insert(size_t bytes);

        /*
            Acknowledge the bytes and unpause the producer if full.
              The tracked bytes is decreased.
        */
        void acknowledge(size_t bytes);

        /*
            Pause the producer if full.
        */
        bool pauseIfFull();

        /*
            Unpause the producer if there's space (or disabled).
        */
        void unpauseIfSpaceAvailable();

    private:

        bool isEnabled_UNLOCKED() {
            return maxBytes != 0;
        }

        bool isFull_UNLOCKED() {
            return bytesSent >= maxBytes;
        }

        void release_UNLOCKED(size_t bytes);

        State getState_UNLOCKED();

        RWLock logLock;
        DcpProducer& producer;
        size_t maxBytes;
        size_t bytesSent;
        size_t ackedBytes;
    };

    /*
        Insert bytes into this producer's buffer log.

        If the log is disabled or the insert was successful returns true.
        Else return false.
    */
    bool bufferLogInsert(size_t bytes);

    /*
        Schedules active stream checkpoint processor task
        for given stream.
    */
    void scheduleCheckpointProcessorTask(stream_t s);

    /*
        Clears active stream checkpoint processor task's queue,
        and cancels the task.
    */
    void cancelCheckpointProcessorTask();

private:

    /**
     * DcpProducerReadyQueue is a std::queue wrapper for managing a
     * queue of vbucket's that are ready for a DCP producer to process.
     * The queue does not allow duplicates and the push_unique method enforces
     * this. The interface is generally customised for the needs of getNextItem
     * and is thread safe as the frontend operations and DCPProducer threads
     * are accessing this data.
     *
     * Internally a std::queue and std::set track the contents and the std::set
     * enables a fast exists method which is used by front-end threads.
     */
    class DcpProducerReadyQueue {
    public:
        bool exists(uint16_t vbucket) {
            LockHolder lh(lock);
            return (queuedValues.count(vbucket) != 0);
        }

        /**
         * Return true and set the ref-param 'frontValue' if the queue is not
         * empty. frontValue is set to the front of the queue.
         */
        bool popFront(uint16_t &frontValue) {
            LockHolder lh(lock);
            if (!readyQueue.empty()) {
                frontValue = readyQueue.front();
                readyQueue.pop();
                queuedValues.erase(frontValue);
                return true;
            }
            return false;
        }

        /**
         * Pop the front item.
         * Safe to call on an empty list
         */
        void pop() {
            LockHolder lh(lock);
            if (!readyQueue.empty()) {
                queuedValues.erase(readyQueue.front());
                readyQueue.pop();
            }
        }

        /**
         * Push the vbucket only if it's not already in the queue
         * Return true if the vbucket was added to the queue.
         */
        bool pushUnique(uint16_t vbucket) {
            LockHolder lh(lock);
            if (queuedValues.count(vbucket) == 0) {
                readyQueue.push(vbucket);
                queuedValues.insert(vbucket);
                return true;
            }
            return false;
        }

        bool empty() {
            LockHolder lh(lock);
            return readyQueue.empty();
        }

    private:
        Mutex lock;

        /* a queue of vbuckets that are ready for producing */
        std::queue<uint16_t> readyQueue;

        /**
         * maintain a std::set of values that are in the readyQueue. find() is
         * performed by front-end threads so we want it to be efficient so just
         * a set lookup is required.
         */
        std::set<uint16_t> queuedValues;
    };

    DcpResponse* getNextItem();

    size_t getItemsRemaining();
    stream_t findStreamByVbid(uint16_t vbid);

    ENGINE_ERROR_CODE maybeSendNoop(struct dcp_message_producers* producers);

    struct {
        rel_time_t sendTime;
        uint32_t opaque;
        uint32_t noopInterval;
        bool pendingRecv;
        bool enabled;
    } noopCtx;

    DcpResponse *rejectResp; // stash response for retry if E2BIG was hit

    bool notifyOnly;
    rel_time_t lastSendTime;
    BufferLog log;

    // Guards all accesses to streams map. If only reading elements in streams
    // (i.e. not adding / removing elements) then can acquire ReadLock, even
    // if a non-const method is called on stream_t.
    RWLock streamsMutex;
    DcpProducerReadyQueue ready;

    std::map<uint16_t, stream_t> streams;

    AtomicValue<size_t> itemsSent;
    AtomicValue<size_t> totalBytesSent;

    ExTask checkpointCreatorTask;
    static const uint32_t defaultNoopInerval;
};

#endif  // SRC_DCP_PRODUCER_H_
