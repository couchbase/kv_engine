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

#include "connhandler.h"
#include "dcp/dcp-types.h"
#include "dcp/ready-queue.h"
#include "dcp/response.h"
#include "dcp/stream_container.h"
#include "ep_engine.h"
#include "monotonic.h"
#include "platform/cacheline_padded.h"

class BackfillManager;
class CheckpointCursor;
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
     * @param startTask If true an internal checkpoint task is created and
     *        started. Test code may wish to defer or manually handle the task
     *        creation.
     */
    DcpProducer(EventuallyPersistentEngine& e,
                const void* cookie,
                const std::string& n,
                uint32_t flags,
                bool startTask);

    virtual ~DcpProducer();

    /**
     * Clears active stream checkpoint processor task's queue, resets its
     * shared reference to the producer and cancels the task.
     */
    void cancelCheckpointCreatorTask();

    ENGINE_ERROR_CODE streamRequest(
            uint32_t flags,
            uint32_t opaque,
            Vbid vbucket,
            uint64_t start_seqno,
            uint64_t end_seqno,
            uint64_t vbucket_uuid,
            uint64_t last_seqno,
            uint64_t next_seqno,
            uint64_t* rollback_seqno,
            dcp_add_failover_log callback,
            boost::optional<cb::const_char_buffer> json) override;

    ENGINE_ERROR_CODE step(struct dcp_message_producers* producers) override;

    ENGINE_ERROR_CODE bufferAcknowledgement(uint32_t opaque,
                                            Vbid vbucket,
                                            uint32_t buffer_bytes) override;

    ENGINE_ERROR_CODE control(uint32_t opaque,
                              cb::const_char_buffer key,
                              cb::const_char_buffer value) override;

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

    void notifySeqnoAvailable(Vbid vbucket, uint64_t seqno);

    void closeStreamDueToVbStateChange(Vbid vbucket, vbucket_state_t state);

    void closeStreamDueToRollback(Vbid vbucket);

    /**
     * This function handles a stream that is detected as slow by the checkpoint
     * remover. Currently we handle the slow stream by switching from in-memory
     * to backfilling.
     *
     * @param vbid vbucket the checkpoint-remover is processing
     * @param cursor the cursor registered in the checkpoint manager which is
     *        slow.
     * @return true if the cursor was removed from the checkpoint manager
     */
    bool handleSlowStream(Vbid vbid, const CheckpointCursor* cursor);

    void closeAllStreams();

    const char *getType() const override;

    void clearQueues();

    size_t getBackfillQueueSize();

    size_t getItemsSent();

    size_t getTotalBytesSent();

    size_t getTotalUncompressedDataSize();

    std::vector<Vbid> getVBVector();

    /**
     * Close the stream for given vbucket stream
     *
     * @param vbucket the if for the vbucket to close
     * @return ENGINE_SUCCESS upon a successful close
     *         ENGINE_NOT_MY_VBUCKET the vbucket stream doesn't exist
     */
    ENGINE_ERROR_CODE closeStream(uint32_t opaque,
                                  Vbid vbucket,
                                  DcpStreamId sid = {}) override;

    void notifyStreamReady(Vbid vbucket);

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

    bool isCompressionEnabled() {
        if (forceValueCompression ||
            engine_.isDatatypeSupported(getCookie(), PROTOCOL_BINARY_DATATYPE_SNAPPY)) {
            return true;
        }

        return false;
    }

    bool isForceValueCompressionEnabled() {
        return forceValueCompression.load();
    }

    bool isSnappyEnabled() {
        return engine_.isDatatypeSupported(getCookie(),
                                           PROTOCOL_BINARY_DATATYPE_SNAPPY);
    }

    bool isCursorDroppingEnabled() const {
        return supportsCursorDropping.load();
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

    void setLastReceiveTime(const rel_time_t time) {
        lastReceiveTime = time;
    }

    bool isDCPExpiryEnabled() const {
        return enableExpiryOpcode;
    }

    /**
     * Tracks the amount of outstanding sent data for a Dcp Producer, alongside
     * how many bytes have been acknowledged by the peer connection.
     *
     * When the buffer becomes full (outstanding >= limit), the producer is
     * paused. Similarly when data is subsequently acknowledged and outstanding
     * < limit; the producer is un-paused.
     */
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
            : producer(p), maxBytes(0), bytesOutstanding(0), ackedBytes(0) {
        }

        /**
         * Change the buffer size to the specified value. A maximum of zero
         * disables buffering.
         */
        void setBufferSize(size_t maxBytes);

        void addStats(ADD_STAT add_stat, const void *c);

        /**
         * Insert N bytes into the buffer.
         *
         * @return false if the log is full, true if the bytes fit or if the
         * buffer log is disabled. The outstanding bytes are increased.
         */
        bool insert(size_t bytes);

        /**
         * Acknowledge the bytes and unpause the producer if full.
         * The outstanding bytes are decreased.
         */
        void acknowledge(size_t bytes);

        /**
         * Pause the producer if full.
         * @return true if the producer was paused; else false.
         */
        bool pauseIfFull();

        /// Unpause the producer if there's space (or disabled).
        void unpauseIfSpaceAvailable();

        size_t getBytesOutstanding() const {
            return bytesOutstanding;
        }

    private:
        bool isEnabled_UNLOCKED() {
            return maxBytes != 0;
        }

        bool isFull_UNLOCKED() {
            return bytesOutstanding >= maxBytes;
        }

        void release_UNLOCKED(size_t bytes);

        State getState_UNLOCKED();

        cb::RWLock logLock;
        DcpProducer& producer;

        /// Capacity of the buffer - maximum number of bytes which can be
        /// outstanding before the buffer is considered full.
        size_t maxBytes;

        /// Number of bytes currently outstanding (in the buffer). Incremented
        /// upon insert(); and then decremented by acknowledge().
        cb::NonNegativeCounter<size_t> bytesOutstanding;

        /// Total number of bytes acknowledeged. Should be non-decreasing in
        /// normal usage; but can be reset to zero when buffer size changes.
        Monotonic<size_t> ackedBytes;
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

    /** Searches the streams map for a stream for vbucket ID. Returns the
     *  found stream, or an empty pointer if none found.
     */
    std::shared_ptr<StreamContainer<std::shared_ptr<Stream>>> findStreams(
            Vbid vbid);

protected:
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
     *  Returns ENGINE_SUCCESS if a noop was sent.
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

        /// How often are DCP noop messages transmitted?
        std::chrono::seconds dcpNoopTxInterval;

        /**
         * True if a DCP NOOP request has been sent and we are waiting for a
         * response.
         */
        Couchbase::RelaxedAtomic<bool> pendingRecv;
        Couchbase::RelaxedAtomic<bool> enabled;
    } noopCtx;

    /// Timestamp of when we last recieved a message from our peer.
    Couchbase::RelaxedAtomic<rel_time_t> lastReceiveTime;

    std::unique_ptr<DcpResponse> getNextItem();

    size_t getItemsRemaining();

    /**
     * Map the end_stream_status_t to one the client can understand.
     * Maps END_STREAM_FILTER_EMPTY to END_STREAM_OK if the client does not
     * understands collections
     * @param cookie client cookie
     * @param status the status to map
     * @param a status safe for the client
     */
    end_stream_status_t mapEndStreamStatus(const void* cookie,
                                           end_stream_status_t status) const;

    /*
     * deletionV1OrV2 unifies the code where a choice is made between triggering
     * a deletion using version 1 or version 2.
     */
    ENGINE_ERROR_CODE deletionV1OrV2(IncludeDeleteTime includeDeleteTime,
                                     MutationResponse& mutationResponse,
                                     dcp_message_producers* producers,
                                     std::unique_ptr<Item> itmCpy,
                                     ENGINE_ERROR_CODE ret);
    /**
     * Set the dead-status of the specified stream associated with the specified
     * vbucket.
     */
    bool setStreamDeadStatus(Vbid vbid,
                             DcpStreamId sid,
                             end_stream_status_t status);

    /**
     * The StreamContainer stores the Stream via a shared_ptr, this is because
     * we have multi-threaded access to the DcpProducer and the possibility
     * that a stream maybe removed from the container whilst a thread is still
     * working on the stream, e.g. closeStream and addStats occurring
     * concurrently.
     */
    using ContainerElement = std::shared_ptr<Stream>;

    /**
     * The StreamsMap maps from vbid to the StreamContainer, which is stored
     * via a shared_ptr. This allows multiple threads to obtain the
     * StreamContainer and for safe destruction to occur.
     */
    using StreamMapValue = std::shared_ptr<StreamContainer<ContainerElement>>;

    using StreamsMap = AtomicUnorderedMap<Vbid, StreamMapValue>;

    /**
     * Attempt to update the map of vb to stream(s) with the new stream
     *
     * @returns true if the vb_conn_map should be updated
     * @throws engine_error if an active stream already exists or logic_error
     *         if the update is not possible.
     */
    bool updateStreamsMap(Vbid vbid,
                          DcpStreamId sid,
                          std::shared_ptr<Stream>& stream);

    // stash response for retry if E2BIG was hit
    std::unique_ptr<DcpResponse> rejectResp;

    bool notifyOnly;

    Couchbase::RelaxedAtomic<bool> enableExtMetaData;
    Couchbase::RelaxedAtomic<bool> forceValueCompression;
    Couchbase::RelaxedAtomic<bool> supportsCursorDropping;
    Couchbase::RelaxedAtomic<bool> sendStreamEndOnClientStreamClose;
    Couchbase::RelaxedAtomic<bool> supportsHifiMFU;
    Couchbase::RelaxedAtomic<bool> enableExpiryOpcode;

    /// Timestamp of when we last transmitted a message to our peer.
    Couchbase::RelaxedAtomic<rel_time_t> lastSendTime;
    BufferLog log;

    // backfill manager object is owned by this class, but use a
    // shared_ptr as the lifetime of the manager is shared between the
    // producer (this class) and BackfillManagerTask (which has a
    // weak_ptr) to this.
    std::shared_ptr<BackfillManager> backfillMgr;

    DcpReadyQueue ready;

    // Map of vbid -> streams. Map itself is atomic (thread-safe).
    StreamsMap streams;
    std::atomic<size_t> itemsSent;
    std::atomic<size_t> totalBytesSent;
    std::atomic<size_t> totalUncompressedDataSize;

    /// Guards access to checkpointCreatorTask, so multiple threads can
    /// safely access  checkpointCreatorTask shared ptr.
    struct CheckpointCreator {
        mutable std::mutex mutex;
        ExTask task;
    };

    // MB-30488: padding to keep mutex from sharing cachelines with
    // unrelated data
    cb::CachelinePadded<CheckpointCreator> checkpointCreator;

    static const std::chrono::seconds defaultDcpNoopTxInterval;

    // Indicates whether the active streams belonging to the DcpProducer should
    // send the value in the response.
    IncludeValue includeValue;
    // Indicates whether the active streams belonging to the DcpProducer should
    // send the xattrs, (if any exist), in the response.
    IncludeXattrs includeXattrs;

    /**
     * Indicates whether the active streams belonging to the DcpProducer should
     * send the tombstone creation time, (if any exist), in the delete messages.
     */
    IncludeDeleteTime includeDeleteTime;

    /* Indicates if the 'checkpoint processor task' should be created.
       NOTE: We always create the checkpoint processor task during regular
             operation. This flag is used for unit testing only */
    const bool createChkPtProcessorTsk;

    /**
     * Does the producer allow the client to create more than one active stream
     * per vbucket (client must enable this feature)
     */
    MultipleStreamRequests multipleStreamRequests{MultipleStreamRequests::No};
};

#endif  // SRC_DCP_PRODUCER_H_
