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

#include "collections/vbucket_filter.h"
#include "dcp/stream.h"
#include <memcached/engine_error.h>
#include <platform/non_negative_counter.h>
#include <spdlog/common.h>

class CheckpointManager;
class VBucket;

/**
 * This class represents an "active" Stream of DCP messages for a given vBucket.
 *
 * "Active" refers to the fact this Stream is generating DCP messages to be sent
 * out to a DCP client which is listening for them.
 *
 * An ActiveStream is essentially a mini state-machine, which starts in
 * StreamState::Pending and then progresses through a sequence of states
 * based on the arguments passed to the stream and the state of the associated
 * VBucket.
 *
 * The expected possible state transitions are described below.
 * Note that error paths, where any state can transition directly to Dead are
 * omitted for brevity (and to avoid cluttering the diagram).
 *
 *               [Pending]
 *                   |
 *                   V
 *             [Backfilling]  <---------------------------\
 *                   |                                    |
 *               Disk only?                               |
 *              /          \                              |
 *            Yes          No                             |
 *             |            |               Pending backfill (cursor dropped)?
 *             |            V                             |
 *             |      Takeover stream?                    |
 *     /-------/      /              \                    |
 *     |             Yes             No                   |
 *     |             |               |                    |
 *     |             V               V                    |
 *     |       [TakeoverSend]    [InMemory] >-------------/
 *     |             |               |
 *     |             V               |
 *     |       [TakeoverWait]        |
 *     |         (pending)           |
 *     |             |               |
 *     |             V               |
 *     |       [TakeoverSend]        |
 *     |             |               |
 *     |             V               |
 *     |       [TakeoverWait]        |
 *     |         (active)            |
 *     |             |               |
 *     \-------------+---------------/
 *                   |
 *                   V
 *                [Dead]
 */
class ActiveStream : public Stream,
                     public std::enable_shared_from_this<ActiveStream> {
public:
    /// The states this ActiveStream object can be in - see diagram in
    /// ActiveStream description.
    enum class StreamState {
        Pending,
        Backfilling,
        InMemory,
        TakeoverSend,
        TakeoverWait,
        Dead
    };

    ActiveStream(EventuallyPersistentEngine* e,
                 std::shared_ptr<DcpProducer> p,
                 const std::string& name,
                 uint32_t flags,
                 uint32_t opaque,
                 VBucket& vbucket,
                 uint64_t st_seqno,
                 uint64_t en_seqno,
                 uint64_t vb_uuid,
                 uint64_t snap_start_seqno,
                 uint64_t snap_end_seqno,
                 IncludeValue includeVal,
                 IncludeXattrs includeXattrs,
                 IncludeDeleteTime includeDeleteTime,
                 Collections::VB::Filter filter);

    virtual ~ActiveStream();

    std::unique_ptr<DcpResponse> next() override;

    void setActive() override {
        LockHolder lh(streamMutex);
        if (isPending()) {
            transitionState(StreamState::Backfilling);
        }
    }

    /// @returns true if state_ is not Dead
    bool isActive() const override;

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

    uint32_t setDead(end_stream_status_t status) override;

    /**
     * Ends the stream.
     *
     * @param status The stream end status
     * @param vbstateLock Exclusive lock to vbstate
     */
    void setDead(end_stream_status_t status,
                 folly::SharedMutex::WriteHolder& vbstateLock);

    StreamState getState() const {
        return state_;
    }

    void notifySeqnoAvailable(uint64_t seqno) override;

    void snapshotMarkerAckReceived();

    void setVBucketStateAckRecieved();

    /// Set the number of backfill items remaining to the given value.
    void setBackfillRemaining(size_t value);

    void setBackfillRemaining_UNLOCKED(size_t value);

    /// Clears the number of backfill items remaining, setting to an empty
    /// (unknown) value.
    void clearBackfillRemaining();

    /**
     * Queues a snapshot marker to be sent - only if there are items in
     * the backfill range which will be sent.
     *
     * Connections which have not negotiated for sync writes will not
     * send prepares or aborts; if the entire backfill is prepares or
     * aborts, then a snapshot marker should not be sent because no
     * items will follow.
     *
     * @param startSeqno start of backfill range
     * @param endSeqno seqno of last item in backfill range
     * @param highCompletedSeqno seqno of last commit/abort in the backfill
     * range
     * @param maxVisibleSeqno seqno of last visible (commit/mutation/system
     * event) item
     * @return If the stream has queued a snapshot marker. If this is false, the
     *         stream determined none of the items in the backfill would be sent
     */
    bool markDiskSnapshot(uint64_t startSeqno,
                          uint64_t endSeqno,
                          boost::optional<uint64_t> highCompletedSeqno,
                          uint64_t maxVisibleSeqno);

    bool backfillReceived(std::unique_ptr<Item> itm,
                          backfill_source_t backfill_source,
                          bool force);

    void completeBackfill();

    bool isCompressionEnabled();

    bool isForceValueCompressionEnabled() const {
        return forceValueCompression == ForceValueCompression::Yes;
    }

    bool isSnappyEnabled() const {
        return snappyEnabled == SnappyEnabled::Yes;
    }

    void addStats(const AddStatFn& add_stat, const void* c) override;

    void addTakeoverStats(const AddStatFn& add_stat,
                          const void* c,
                          const VBucket& vb);

    /* Returns a count of how many items are outstanding to be sent for this
     * stream's vBucket.
     */
    size_t getItemsRemaining();

    uint64_t getLastReadSeqno() const;

    uint64_t getLastSentSeqno() const;

    // Defined in active_stream_impl.h to remove the need to include the
    // producer header here
    template <typename... Args>
    void log(spdlog::level::level_enum severity,
             const char* fmt,
             Args... args) const;

    // Runs on ActiveStreamCheckpointProcessorTask
    void nextCheckpointItemTask();

    /**
     * Function to handle a slow stream that is supposedly hogging memory in
     * checkpoint mgr. Currently we handle the slow stream by switching from
     * in-memory to backfilling
     *
     * @return true if cursor is dropped; else false
     */
    bool handleSlowStream();

    /// @return true if both includeValue and includeXattributes are set to No,
    /// otherwise return false.
    bool isKeyOnly() const {
        // IncludeValue::NoWithUnderlyingDatatype doesn't allow key-only,
        // as we need to fetch the datatype also (which is not present in
        // revmeta for V0 documents, so in general still requires fetching
        // the body).
        return (includeValue == IncludeValue::No) &&
               (includeXattributes == IncludeXattrs::No);
    }

    const Cursor& getCursor() const override {
        return cursor;
    }

    std::string getStreamTypeName() const override;

    std::string getStateName() const override;

    bool compareStreamId(cb::mcbp::DcpStreamId id) const override {
        return id == sid;
    }

    /**
     * Result of the getOutstandingItems function
     */
    struct OutstandingItemsResult {
        /**
         * The type of Checkpoint that these items belong to. Defaults to Memory
         * as this results in the most fastidious error checking on the replica
         */
        CheckpointType checkpointType = CheckpointType::Memory;
        std::vector<queued_item> items;
        /**
         * The HCS. Set only for CheckpointType::Disk, ie when a Producer
         * streams a disk-snapshot from memory.
         */
        boost::optional<uint64_t> highCompletedSeqno;

        /**
         * The visibleSeqno used to 'seed' the processItems loop
         */
        uint64_t visibleSeqno{0};
    };

    /**
     * Process a seqno ack against this stream.
     *
     * @param consumerName the name of the consumer acking
     * @param preparedSeqno the seqno that the consumer is acking
     */
    ENGINE_ERROR_CODE seqnoAck(const std::string& consumerName,
                               uint64_t preparedSeqno);

    static std::string to_string(StreamState type);

    /**
     * Notifies the stream that a scheduled backfill completed
     * without providing any items to backfillReceived, and
     * without marking a disk snapshot.
     *
     * If the cursor has been dropped, re-registers it to allow the stream
     * to transition to memory.
     *
     * @param lastReadSeqno last seqno in backfill range
     */
    void notifyEmptyBackfill(uint64_t lastSeenSeqno);

    void notifyEmptyBackfill_UNLOCKED(uint64_t lastSeenSeqno);

protected:
    /**
     * @param vb reference to the associated vbucket
     *
     * @return the outstanding items for the stream's checkpoint cursor and
     *         checkpoint type.
     */
    virtual ActiveStream::OutstandingItemsResult getOutstandingItems(
            VBucket& vb);

    /**
     * Given a set of queued items, create mutation response for each item,
     * and pass onto the producer associated with this stream.
     *
     * @param outstandingItemsResult vector of Items and Checkpoint type from
     * which they came
     * @param streamMutex Lock
     */
    void processItems(OutstandingItemsResult& outstandingItemsResult,
                      const LockHolder& streamMutex);

    /**
     * Should the given item be sent out across this stream?
     * @returns true if the item should be sent, false if it should be ignored.
     */
    bool shouldProcessItem(const Item& it);

    bool nextCheckpointItem();

    std::unique_ptr<DcpResponse> nextQueuedItem();

    /**
     * Create a DcpResponse message to send to the replica from the given item.
     *
     * @param item The item to turn into a DcpResponse
     * @param sendCommitSyncWriteAs Should we send a mutation instead of a
     *                                    commit? This should be the case if we
     *                                    are backfilling.
     * @return a DcpResponse to represent the item. This will be either a
     *         MutationResponse, SystemEventProducerMessage, CommitSyncWrite or
     *         AbortSyncWrite.
     */
    std::unique_ptr<DcpResponse> makeResponseFromItem(
            const queued_item& item,
            SendCommitSyncWriteAs sendCommitSyncWriteAs);

    /* The transitionState function is protected (as opposed to private) for
     * testing purposes.
     */
    void transitionState(StreamState newState);

    /**
     * Registers a cursor with a given CheckpointManager.
     * The result of calling the function is that it sets the pendingBackfill
     * flag, if another backfill is required.  It also sets the curChkSeqno to
     * be at the position the new cursor is registered.
     *
     * @param chkptmgr  The CheckpointManager the cursor will be registered to.
     * @param lastProcessedSeqno  The last processed seqno.
     */
    virtual void registerCursor(CheckpointManager& chkptmgr,
                                uint64_t lastProcessedSeqno);

    /**
     * Unlocked variant of nextCheckpointItemTask caller must obtain
     * streamMutex and pass a reference to it
     * @param streamMutex reference to lockholder
     */
    void nextCheckpointItemTask(const LockHolder& streamMutex);

    bool supportSyncReplication() const {
        return syncReplication == SyncReplication::SyncReplication;
    }

    bool supportSyncWrites() const {
        return syncReplication != SyncReplication::No;
    }

    // The current state the stream is in.
    // Atomic to allow reads without having to acquire the streamMutex.
    std::atomic<StreamState> state_{StreamState::Pending};

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
    AtomicMonotonic<uint64_t, ThrowExceptionPolicy> lastReadSeqno;

    /* backfillRemaining is a stat recording the amount of items remaining to
     * be read from disk.
     * Before the number of items to be backfilled has been determined (disk
     * scanned) it is empty.
     * Guarded by streamMutex.
     */
    boost::optional<cb::NonNegativeCounter<size_t>> backfillRemaining;

    std::unique_ptr<DcpResponse> backfillPhase(std::lock_guard<std::mutex>& lh);

    Cursor cursor;

private:
    std::unique_ptr<DcpResponse> next(std::lock_guard<std::mutex>& lh);

    std::unique_ptr<DcpResponse> inMemoryPhase();

    std::unique_ptr<DcpResponse> takeoverSendPhase();

    std::unique_ptr<DcpResponse> takeoverWaitPhase();

    std::unique_ptr<DcpResponse> deadPhase();

    /**
     * Pushes the items of a snapshot to the readyQ.
     *
     * @param checkpointType The type of checkpoint (Disk/Memory)
     * @param snapshot The items to be streamed
     * @param highCompletedSeqno (optional) Required for CheckpointType::Disk
     * @param maxVisibleSeqno the maximum visible seq (not prepare/abort)
     */
    void snapshot(CheckpointType checkpointType,
                  std::deque<std::unique_ptr<DcpResponse>>& snapshot,
                  boost::optional<uint64_t> highCompletedSeqno,
                  uint64_t maxVisibleSeqno);

    void endStream(end_stream_status_t reason);

    /* reschedule = FALSE ==> First backfill on the stream
     * reschedule = TRUE ==> Schedules another backfill on the stream that has
     *                       finished backfilling once and still in
     *                       STREAM_BACKFILLING state or in STREAM_IN_MEMORY
     *                       state.
     * Note: Expects the streamMutex to be acquired when called
     */
    void scheduleBackfill_UNLOCKED(bool reschedule);

    std::string getEndStreamStatusStr(end_stream_status_t status);

    bool isCurrentSnapshotCompleted() const;

    /**
     * Drop the cursor registered with the checkpoint manager. Used during
     * cursor dropping. Upon failure to drop the cursor, puts stream to
     * dead state and notifies the producer connection
     * Note: Expects the streamMutex to be acquired when called
     *
     * @return true if cursor is dropped; else false
     */
    bool dropCheckpointCursor_UNLOCKED();

    /**
     * Notifies the producer connection that the stream has items ready to be
     * pick up.
     *
     * @param force Indiciates if the function should notify the connection
     *              irrespective of whether the connection already knows that
     *              the items are ready to be picked up. Default is 'false'
     */
    void notifyStreamReady(bool force = false);

    /**
     * Helper function that tries to takes the ownership of the vbucket
     * (temporarily) and then removes the checkpoint cursor held by the stream.
     */
    bool removeCheckpointCursor();

    /**
     * Decides what log level must be used for (active) stream state
     * transitions
     *
     * @param currState current state of the stream
     * @param newState new state of the stream
     *
     * @return log level
     */
    spdlog::level::level_enum getTransitionStateLogLevel(StreamState currState,
                                                         StreamState newState);

    /**
     * Performs the basic actions for closing a stream (ie, queueing a
     * stream-end message and notifying the connection).
     *
     * @param status The end stream status
     */
    void setDeadInner(end_stream_status_t status);

    /**
     * Remove the acks from the ActiveDurabilityMonitor for this stream.
     *
     * @param vbstateLock (optional) Exclusive lock to vbstate. The function
     *     acquires the lock if not provided.
     */
    void removeAcksFromDM(
            boost::optional<folly::SharedMutex::WriteHolder&> vbstateLock = {});

    /* The last sequence number queued from memory, but is yet to be
       snapshotted and put onto readyQ */
    std::atomic<uint64_t> lastReadSeqnoUnSnapshotted;

    //! The last sequence number sent to the network layer
    std::atomic<uint64_t> lastSentSeqno;

    //! The last known seqno pointed to by the checkpoint cursor
    std::atomic<uint64_t> curChkSeqno;

    /**
     * Should the next snapshot marker have the 'checkpoint' flag
     * (MARKER_FLAG_CHK) set?
     * See comments in processItems() for usage of this variable.
     */
    bool nextSnapshotIsCheckpoint = false;

    //! The current vbucket state to send in the takeover stream
    vbucket_state_t takeoverState;

    //! The amount of items that have been sent during the memory phase
    std::atomic<size_t> itemsFromMemoryPhase;

    //! Whether or not this is the first snapshot marker sent
    // @TODO - update to be part of the state machine.
    bool firstMarkerSent;

    /**
     * Indicates if the stream is currently waiting for a snapshot to be
     * acknowledged by the peer. Incremented when forming SnapshotMarkers in
     * TakeoverSend phase, and decremented when SnapshotMarkerAck is received
     * from the peer.
     */
    std::atomic<int> waitForSnapshot;

    EventuallyPersistentEngine* const engine;
    const std::weak_ptr<DcpProducer> producerPtr;

    struct {
        std::atomic<size_t> bytes;
        std::atomic<size_t> items;
    } bufferedBackfill;

    /// Records the time at which the TakeoverSend phase begins.
    std::atomic<rel_time_t> takeoverStart;

    /**
     * Maximum amount of time the TakeoverSend phase is permitted to take before
     * TakeoverSend is considered "backed up" and new frontend mutations will
     * paused.
     */
    const size_t takeoverSendMaxTime;

    //! Last snapshot end seqno sent to the DCP client
    std::atomic<uint64_t> lastSentSnapEndSeqno;

    /* Flag used by checkpointCreatorTask that is set before all items are
       extracted for given checkpoint cursor, and is unset after all retrieved
       items are added to the readyQ */
    std::atomic<bool> chkptItemsExtractionInProgress;

    // Whether the responses sent using this stream should contain the value
    const IncludeValue includeValue;
    // Whether the responses sent using the stream should contain the xattrs
    // (if any exist)
    const IncludeXattrs includeXattributes;

    // Will the stream send dcp deletions with delete-times?
    const IncludeDeleteTime includeDeleteTime;

    // Will the stream encode the CollectionID in the key?
    const DocKeyEncodesCollectionId includeCollectionID;

    // Will the stream be able to output expiry opcodes?
    const EnableExpiryOutput enableExpiryOutput;

    /// Is Snappy compression supported on this connection?
    const SnappyEnabled snappyEnabled;

    /// Should items be forcefully compressed on this stream?
    const ForceValueCompression forceValueCompression;

    /// Does this stream support synchronous replication (i.e. acking Prepares)?
    /**
     * What level of SyncReplication does this stream Support:
     *  - None
     *  - SyncWrites: Sending Prepares/Commits/Aborts
     *  - SyncReplication: SyncWrites + Acking Prepares
     */
    const SyncReplication syncReplication;

    /**
     * The filter the stream will use to decide which keys should be transmitted
     */
    Collections::VB::Filter filter;

protected:
    /**
     * A stream-ID which is defined if the producer is using enabled to allow
     * many streams-per-vbucket
     */
    const cb::mcbp::DcpStreamId sid;

private:
    /**
     * A prefix to use in all stream log messages
     */
    std::string logPrefix;
};
