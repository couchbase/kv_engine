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

#include "atomic_unordered_map.h"

#include "collections/filter.h"
#include "connhandler.h"
#include "dcp/dcp-types.h"

class BackfillManager;
class DcpResponse;

class DcpProducer : public ConnHandler,
                    public std::enable_shared_from_this<DcpProducer> {
public:

    /**
     * Construct a DCP Producer
     *
     * @param e The engine.
     * @param cookie Cookie of the connection creating the producer.
     * @param n A name chosen by the client.
     * @param flags The DCP_OPEN flags (as per mcbp).
     * @param jsonFilter JSON document containing filter configuration.
     * @param startTask If true an internal checkpoint task is created and
     *        started. Test code may wish to defer or manually handle the task
     *        creation.
     */
    DcpProducer(EventuallyPersistentEngine& e,
                const void* cookie,
                const std::string& n,
                uint32_t flags,
                Collections::Filter filter,
                bool startTask);

    virtual ~DcpProducer();

    ENGINE_ERROR_CODE streamRequest(uint32_t flags, uint32_t opaque,
                                    uint16_t vbucket, uint64_t start_seqno,
                                    uint64_t end_seqno, uint64_t vbucket_uuid,
                                    uint64_t last_seqno, uint64_t next_seqno,
                                    uint64_t *rollback_seqno,
                                    dcp_add_failover_log callback) override;

    ENGINE_ERROR_CODE getFailoverLog(uint32_t opaque, uint16_t vbucket,
                                     dcp_add_failover_log callback) override;

    ENGINE_ERROR_CODE step(struct dcp_message_producers* producers) override;

    ENGINE_ERROR_CODE bufferAcknowledgement(uint32_t opaque, uint16_t vbucket,
                                            uint32_t buffer_bytes) override;

    ENGINE_ERROR_CODE control(uint32_t opaque, const void* key, uint16_t nkey,
                              const void* value, uint32_t nvalue) override;

    /**
     * Sub-classes must implement a method that processes a response
     * to a request initiated by itself.
     *
     * @param resp A mcbp response message to process.
     * @returns true/false which will be converted to SUCCESS/DISCONNECT by the
     *          engine.
     */
    bool handleResponse(const protocol_binary_response_header* resp) override;

    void addStats(ADD_STAT add_stat, const void *c) override;

    void addTakeoverStats(ADD_STAT add_stat, const void* c, const VBucket& vb);

    void aggregateQueueStats(ConnCounter& aggregator) override;

    void setDisconnect() override;

    void notifySeqnoAvailable(uint16_t vbucket, uint64_t seqno);

    void closeStreamDueToVbStateChange(uint16_t vbucket, vbucket_state_t state);

    void closeStreamDueToRollback(uint16_t vbucket);

    /* This function handles a stream that is detected as slow by the checkpoint
       remover. Currently we handle the slow stream by switching from in-memory
       to backfilling */
    bool handleSlowStream(uint16_t vbid, const std::string &name);

    void closeAllStreams();

    const char *getType() const override;

    void clearQueues();

    size_t getBackfillQueueSize();

    size_t getItemsSent();

    size_t getTotalBytesSent();

    size_t getTotalUncompressedDataSize();

    std::vector<uint16_t> getVBVector(void);

    /**
     * Close the stream for given vbucket stream
     *
     * @param vbucket the if for the vbucket to close
     * @return ENGINE_SUCCESS upon a successful close
     *         ENGINE_NOT_MY_VBUCKET the vbucket stream doesn't exist
     */
    ENGINE_ERROR_CODE closeStream(uint32_t opaque, uint16_t vbucket) override;

    void notifyStreamReady(uint16_t vbucket);

    void notifyBackfillManager();
    bool recordBackfillManagerBytesRead(size_t bytes, bool force);
    void recordBackfillManagerBytesSent(size_t bytes);
    void scheduleBackfillManager(VBucket& vb,
                                 std::shared_ptr<ActiveStream> s,
                                 uint64_t start,
                                 uint64_t end);

    bool isExtMetaDataEnabled () {
        return enableExtMetaData;
    }

    bool isValueCompressionEnabled() {
        return enableValueCompression;
    }

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

        cb::RWLock logLock;
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
    void scheduleCheckpointProcessorTask(std::shared_ptr<ActiveStream> s);

    /*
        Clears active stream checkpoint processor task's queue.
    */
    void clearCheckpointProcessorTaskQueues();

protected:
    /** Searches the streams map for a stream for vbucket ID. Returns the
     *  found stream, or an empty pointer if none found.
     */
    std::shared_ptr<Stream> findStream(uint16_t vbid);

    /** We may disconnect if noop messages are enabled and the last time we
     *  received any message (including a noop) exceeds the dcpTimeout.
     *  Returns ENGINE_DISCONNECT if noop messages are enabled and the timeout
     *  is exceeded.
     *  Returns ENGINE_FAILED if noop messages are disabled, or if the timeout
     *  is not exceeded.  In this case continue without disconnecting.
     */
    ENGINE_ERROR_CODE maybeDisconnect();

    /** We may send a noop if a noop acknowledgement is not pending and
     *  we have exceeded the dcpNoopTxInterval since we last sent a noop.
     *  Returns ENGINE_WANT_MORE if a noop was sent.
     *  Returns ENGINE_FAILED if a noop is not required to be sent.
     *  This occurs if noop messages are disabled, or because we have already
     *  sent a noop and we are awaiting a receive, or because the time interval
     *  has not passed.
     */
    ENGINE_ERROR_CODE maybeSendNoop(struct dcp_message_producers* producers);

    /**
     * Create the ActiveStreamCheckpointProcessorTask and assign to
     * checkpointCreatorTask
     */
    void createCheckpointProcessorTask();

    /**
     * Schedule the checkpointCreatorTask on the ExecutorPool
     */
    void scheduleCheckpointProcessorTask();

    struct {
        rel_time_t sendTime;
        uint32_t opaque;
        std::chrono::seconds dcpIdleTimeout;
        std::chrono::seconds dcpNoopTxInterval;
        Couchbase::RelaxedAtomic<bool> pendingRecv;
        Couchbase::RelaxedAtomic<bool> enabled;
    } noopCtx;

    Couchbase::RelaxedAtomic<rel_time_t> lastReceiveTime;

    std::unique_ptr<DcpResponse> getNextItem();

    size_t getItemsRemaining();

    std::string priority;

    // stash response for retry if E2BIG was hit
    std::unique_ptr<DcpResponse> rejectResp;

    bool notifyOnly;

    Couchbase::RelaxedAtomic<bool> enableExtMetaData;
    Couchbase::RelaxedAtomic<bool> enableValueCompression;
    Couchbase::RelaxedAtomic<bool> supportsCursorDropping;

    Couchbase::RelaxedAtomic<rel_time_t> lastSendTime;
    BufferLog log;

    // backfill manager object is owned by this class, but use a
    // shared_ptr as the lifetime of the manager is shared between the
    // producer (this class) and BackfillManagerTask (which has a
    // weak_ptr) to this.
    std::shared_ptr<BackfillManager> backfillMgr;

    DcpReadyQueue ready;

    // Map of vbid -> stream. Map itself is atomic (thread-safe).
    typedef AtomicUnorderedMap<uint16_t, std::shared_ptr<Stream>> StreamsMap;
    StreamsMap streams;

    std::atomic<size_t> itemsSent;
    std::atomic<size_t> totalBytesSent;
    std::atomic<size_t> totalUncompressedDataSize;

    ExTask checkpointCreatorTask;
    static const std::chrono::seconds defaultDcpNoopTxInterval;

    // Indicates whether the active streams belonging to the DcpProducer should
    // send the value in the response.
    IncludeValue includeValue;
    // Indicates whether the active streams belonging to the DcpProducer should
    // send the xattrs, (if any exist), in the response.
    IncludeXattrs includeXattrs;

    /**
     * The producer owns a "bucket" level filter which is used to build the
     * actual data filter (Collections::VB::Filter) per VB stream at request
     * time.
     */
    Collections::Filter filter;
};

#endif  // SRC_DCP_PRODUCER_H_
