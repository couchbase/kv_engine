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

#include "collections/vbucket_filter.h"
#include "dcp/stream.h"
#include "utilities/testing_hook.h"
#include <memcached/engine_error.h>
#include <platform/non_negative_counter.h>
#include <relaxed_atomic.h>
#include <spdlog/common.h>
#include <optional>

class CheckpointManager;
class VBucket;
enum class ValueFilter;

struct CheckpointSnapshotRange;

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

    /// What order are items returned for this backfill?
    enum class BackfillType {
        /// In the original sequence number order.
        InOrder,
        /// Out of the original sequence number order.
        OutOfSequenceOrder,
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
                 IncludeDeletedUserXattrs includeDeletedUserXattrs,
                 Collections::VB::Filter filter);

    ~ActiveStream() override;

    /**
     * Get the next item for this stream
     *
     * @param producer Reference to the calling DcpProducer that may be used to
     *                 update the BufferLog (nextQueuedItem) or notify the
     *                 producer (notifyStream). This helps us avoid promotion of
     *                 the producerPtr weak_ptr on the memcached worker path
     *                 which reduces cache contention that can be observed on
     *                 this path and the AuxIO backfill/checkpoint processor
     *                 path.
     * @return DcpResponse to ship to the consumer (via the calling DcpProducer)
     */
    std::unique_ptr<DcpResponse> next(DcpProducer& producer);

    void setActive() override {
        std::lock_guard<std::mutex> lh(streamMutex);
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

    uint32_t setDead(cb::mcbp::DcpStreamEndStatus status) override;

    /**
     * Ends the stream.
     *
     * @param status The stream end status
     * @param vbstateLock Exclusive lock to vbstate
     */
    void setDead(cb::mcbp::DcpStreamEndStatus status,
                 folly::SharedMutex::WriteHolder& vbstateLock);

    StreamState getState() const {
        return state_;
    }

    /**
     * Notify the producer that the stream is ready.
     *
     * @param producer reference to the calling producer to avoid promoting the
     *                 producerPtr weak_ptr
     */
    void notifySeqnoAvailable(DcpProducer& producer);

    void snapshotMarkerAckReceived();

    /**
     * Process SetVBucketState response
     *
     * @param producer reference to the calling DcpProducer
     */
    void setVBucketStateAckRecieved(DcpProducer& producer);

    /// Set the number of backfill items remaining to the given value.
    void setBackfillRemaining(size_t value);

    void setBackfillRemaining_UNLOCKED(size_t value);

    /// Increment the number of times a backfill is paused.
    void incrementNumBackfillPauses();

    uint64_t getNumBackfillPauses() {
        return numBackfillPauses;
    }

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
     * @param timestamp of the disk snapshot (if available)
     * @return If the stream has queued a snapshot marker. If this is false, the
     *         stream determined none of the items in the backfill would be sent
     */
    bool markDiskSnapshot(uint64_t startSeqno,
                          uint64_t endSeqno,
                          std::optional<uint64_t> highCompletedSeqno,
                          uint64_t maxVisibleSeqno,
                          std::optional<uint64_t> timestamp);

    /**
     * Queues a single "Out of Seqno Order" marker with the 'start' flag
     * into the ready queue
     *
     * @param endSeqno the end of the disk snapshot - used for cursor
     *        registration
     */
    bool markOSODiskSnapshot(uint64_t endSeqno);

    bool backfillReceived(std::unique_ptr<Item> itm,
                          backfill_source_t backfill_source);

    /**
     * @param runtime The total runtime the backfill took, measured as active
     * time executing (i.e. total BackfillManagerTask::run() durations for
     * this backfill)
     * @param diskBytesRead The total number of bytes read from disk during
     *        this backfill.
     */
    void completeBackfill(std::chrono::steady_clock::duration runtime,
                          size_t diskBytesRead);

    /**
     * Queues a single "Out of Seqno Order" marker with the 'end' flag
     * into the ready queue.
     *
     * @param maxSeqno the maximum seqno of the snapshot supplying the OSO
     *        backfill. A SeqnoAdvanced maybe sent if the last backfilled
     *        item is not the maxSeqno item
     * @param runtime The total runtime the backfill took, measured as active
     *        time executing (i.e. total BackfillManagerTask::run() durations
     *        for this backfill).
     * @param diskBytesRead The total number of bytes read from disk during
     *        this backfill.
     */
    void completeOSOBackfill(uint64_t maxSeqno,
                             std::chrono::steady_clock::duration runtime,
                             size_t diskBytesRead);

    bool isCompressionEnabled() const;

    bool isForceValueCompressionEnabled() const {
        return forceValueCompression == ForceValueCompression::Yes;
    }

    bool isSnappyEnabled() const {
        return snappyEnabled == SnappyEnabled::Yes;
    }

    void addStats(const AddStatFn& add_stat, const CookieIface* c) override;

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

    /// @return true if IncludeValue/IncludeXattrs/IncludeDeletedUserXattrs are
    /// set to No, otherwise return false.
    bool isKeyOnly() const {
        // IncludeValue::NoWithUnderlyingDatatype doesn't allow key-only,
        // as we need to fetch the datatype also (which is not present in
        // revmeta for V0 documents, so in general still requires fetching
        // the body).
        return (includeValue == IncludeValue::No) &&
               (includeXattributes == IncludeXattrs::No) &&
               (includeDeletedUserXattrs == IncludeDeletedUserXattrs::No);
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
        // OutstandingItemsResult ctor and dtor required to forward declare
        // CheckpointSnapshotRange
        OutstandingItemsResult();
        ~OutstandingItemsResult();

        /**
         * Optional state required when sending a checkpoint of type Disk
         * (i.e. when a Producer streams a disk-snapshot from memory.
         */
        struct DiskCheckpointState {
            /**
             * The HCS of the original disk snapshot
             */
            uint64_t highCompletedSeqno;
        };

        /**
         * The type of Checkpoint that these items belong to. Defaults to Memory
         * as this results in the most fastidious error checking on the replica
         */
        CheckpointType checkpointType = CheckpointType::Memory;
        std::vector<queued_item> items;

        /**
         * Disk checkpoint state is optional as it is only required for disk
         * checkpoints.
         */
        std::optional<DiskCheckpointState> diskCheckpointState;

        /**
         * The visibleSeqno used to 'seed' the processItems loop
         */
        uint64_t visibleSeqno{0};

        /**
         * The snapshot bounds for the checkpoints we are going to send
         */
        std::vector<CheckpointSnapshotRange> ranges;
    };

    /**
     * Process a seqno ack against this stream.
     *
     * @param consumerName the name of the consumer acking
     * @param preparedSeqno the seqno that the consumer is acking
     */
    cb::engine_errc seqnoAck(const std::string& consumerName,
                             uint64_t preparedSeqno);

    static std::string to_string(StreamState type);

    bool collectionAllowed(DocKey key) const;

    /**
     * reassess the streams required privileges and call endStream if required
     * @param producer reference to the calling DcpProducer
     */
    bool endIfRequiredPrivilegesLost(DcpProducer& producer);

    std::unique_ptr<DcpResponse> makeEndStreamResponse(
            cb::mcbp::DcpStreamEndStatus);

    bool isDiskOnly() const;

    bool isTakeoverStream() const;

    PointInTimeEnabled isPointInTimeEnabled() const {
        return pitrEnabled;
    }

    /**
     * Used to set the last read seqno from a scan of the data store layer.
     * (This is for external use only and ensures that
     * maxScanSeqno is zero prior to being set).
     * @param seqno last read seqno during a data store layer scan
     */
    void setBackfillScanLastRead(uint64_t seqno) {
        std::unique_lock<std::mutex> lh(streamMutex);
        /*
         * maxScanSeqno should only be modified once per
         * backfill
         */
        Expects(maxScanSeqno == 0);
        maxScanSeqno = seqno;
    }

    /**
     * Method to get the collections filter of the stream
     * @return the filter object
     */
    const Collections::VB::Filter& getFilter() const {
        return filter;
    }

    bool supportSyncWrites() const {
        return syncReplication != SyncReplication::No;
    }

    /**
     * @return the KVStore ValueFilter for this stream
     */
    ValueFilter getValueFilter() const;

    /**
     * Set the end_seqno_ of stream to the value of seqno
     * @param seqno
     */
    void setEndSeqno(uint64_t seqno);

    const std::string& getLogPrefix() const {
        return logPrefix;
    }

    // Introduced in MB-45757 for testing a race condition on invalidate-cursor
    TestingHook<> removeCursorPreLockHook;

protected:
    void clear_UNLOCKED();

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
    void notifyEmptyBackfill_UNLOCKED(uint64_t lastSeenSeqno);

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
                      const std::lock_guard<std::mutex>& streamMutex);

    /**
     * Should the given item be sent out across this stream?
     * @returns true if the item should be sent, false if it should be ignored.
     */
    bool shouldProcessItem(const Item& it);

    /**
     * Schedules the checkpointProcessorTask of the DcpProducer if there are
     * items to send.
     *
     * @param producer reference to the DcpProducer to schedule the
     *                 checkpointProcessorTask
     * @return true if there are items to be send from checkpoint(s)
     */
    bool nextCheckpointItem(DcpProducer& producer);

    /**
     * Get the next queued item.
     *
     * @param producer Producer for tracking the size against the BufferLog
     * @return DcpResponse to ship to the consumer
     */
    std::unique_ptr<DcpResponse> nextQueuedItem(DcpProducer&);

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
    void nextCheckpointItemTask(const std::lock_guard<std::mutex>& streamMutex);

    bool supportSyncReplication() const {
        return syncReplication == SyncReplication::SyncReplication;
    }

    /**
     * An OSO backfill is not always possible, this method will try to
     * schedule one.
     * @param the owning producer
     * @param the vbucket for the stream
     * @return true if the backfill was scheduled
     */
    bool tryAndScheduleOSOBackfill(DcpProducer& producer, VBucket& vb);

    bool isCollectionEnabledStream() const {
        return !filter.isLegacyFilter();
    }

    /// Common helper function for completing backfills.
    void completeBackfillInner(BackfillType backfillType,
                               uint64_t maxSeqno,
                               std::chrono::steady_clock::duration runtime,
                               size_t diskBytesRead);

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
        std::atomic<size_t> memory = 0;
        std::atomic<size_t> disk = 0;
        std::atomic<size_t> sent = 0;
    } backfillItems;

    struct Labeller {
        std::string getLabel(const char* name) const;
        const ActiveStream& stream;
    };
    /* The last sequence number queued from disk or memory and is
       snapshotted and put onto readyQ */
    ATOMIC_MONOTONIC4(uint64_t, lastReadSeqno, Labeller, ThrowExceptionPolicy);

    /* backfill ById or BySeqno updates this member during the scan, then
       this value is copied into the lastReadSeqno member when completed */
    uint64_t lastBackfilledSeqno{0};

    /* backfillRemaining is a stat recording the amount of items remaining to
     * be read from disk.
     * Before the number of items to be backfilled has been determined (disk
     * scanned) it is empty.
     * Guarded by streamMutex.
     */
    std::optional<cb::NonNegativeCounter<size_t>> backfillRemaining;

    std::unique_ptr<DcpResponse> backfillPhase(DcpProducer& producer,
                                               std::lock_guard<std::mutex>& lh);

    Cursor cursor;

    // MB-37468: Test only hooks set via Mock class
    TestingHook<> completeBackfillHook;
    TestingHook<> nextHook;
    TestingHook<> takeoverSendPhaseHook;

    // Whether the responses sent using this stream should contain the body
    const IncludeValue includeValue;

    // Whether the responses sent using the stream should contain the xattrs (if
    // any)
    const IncludeXattrs includeXattributes;

    // Will the stream include user-xattrs (if any) at sending dcp (normal/sync)
    // deletions?
    const IncludeDeletedUserXattrs includeDeletedUserXattrs;

private:
    std::unique_ptr<DcpResponse> inMemoryPhase(DcpProducer& producer);

    std::unique_ptr<DcpResponse> takeoverSendPhase(DcpProducer& producer);

    std::unique_ptr<DcpResponse> takeoverWaitPhase(DcpProducer& producer);

    std::unique_ptr<DcpResponse> deadPhase(DcpProducer& producer);

    /**
     * Pushes the items of a snapshot to the readyQ.
     *
     * @param checkpointType The type of checkpoint (Disk/Memory)
     * @param items The items to be streamed
     * @param diskCheckpointState (optional) state sent for disk checkpoints
     * @param maxVisibleSeqno the maximum visible seq (not prepare/abort)
     * @param highNonVisibleSeqno the snapEnd seqno that includes any non
     * visible mutations i.e. prepares and aborts. This is only used when
     * collections is enabled and sync writes are not supported on the stream.
     */
    void snapshot(CheckpointType checkpointType,
                  std::deque<std::unique_ptr<DcpResponse>>& items,
                  std::optional<OutstandingItemsResult::DiskCheckpointState>
                          diskCheckpointState,
                  uint64_t maxVisibleSeqno,
                  std::optional<uint64_t> highNonVisibleSeqno);

    void endStream(cb::mcbp::DcpStreamEndStatus reason);

    /* reschedule = FALSE ==> First backfill on the stream
     * reschedule = TRUE ==> Schedules another backfill on the stream that has
     *                       finished backfilling once and still in
     *                       STREAM_BACKFILLING state or in STREAM_IN_MEMORY
     *                       state.
     * Note: Expects the streamMutex to be acquired when called
     */
    void scheduleBackfill_UNLOCKED(DcpProducer& producer, bool reschedule);

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
     * @param producer optional pointer to the DcpProducer owning this stream.
     *                 Supplied in some cases to reduce the number of times that
     *                 we promote the weak_ptr to the DcpProducer (producerPtr).
     */
    void notifyStreamReady(bool force = false, DcpProducer* producer = nullptr);

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
    static spdlog::level::level_enum getTransitionStateLogLevel(
            StreamState currState, StreamState newState);

    /**
     * Performs the basic actions for closing a stream (ie, queueing a
     * stream-end message and notifying the connection).
     *
     * @param status The end stream status
     */
    void setDeadInner(cb::mcbp::DcpStreamEndStatus status);

    /**
     * Remove the acks from the ActiveDurabilityMonitor for this stream.
     *
     * @param vbstateLock (optional) Exclusive lock to vbstate. The function
     *     acquires the lock if not provided.
     */
    void removeAcksFromDM(
            folly::SharedMutex::WriteHolder* vbstateLock = nullptr);

    /**
     * Checks if a DcpSeqnoAdvanced can be sent on this stream
     * @return true if enabled; false otherwise
     */
    bool isSeqnoAdvancedEnabled() const;

    /**
     * Method to check if there's a seqno gap between the snapEnd and the seqno
     * of the last item in the snapshot.
     * @param how far the stream has read, e.g. the last read item from
     *        checkpoint or last item read from disk. This value is used to
     *        ensure the stream has now reached the end of the current snapshot.
     * @return true if lastReadSeqno < snapEnd and snapEnd == streamSeqno
     */
    bool isSeqnoGapAtEndOfSnapshot(uint64_t streamSeqno) const;

    /**
     * Method to check if a SeqnoAdvanced is needed at the end of backfill
     * snapshot
     * @return true if a SeqnoAdvanced is needed
     */
    bool isSeqnoAdvancedNeededBackFill() const;

    /**
     * Method to enqueue a SeqnoAdvanced op with the seqno being the value of
     * lastSentSnapEndSeqno
     */
    void queueSeqnoAdvanced();

    /**
     * Enqueue a single snapshot + seqno advance
     * @param checkpointType type of checkpoint which triggered this snapshot
     * @param start value of snapshot start
     * @param end value of snapshot end
     */
    void sendSnapshotAndSeqnoAdvanced(CheckpointType checkpointType,
                                      uint64_t start,
                                      uint64_t end);

    /**
     * If firstMarkerSent is false then the startSeqno of a snapshot may need
     * adjusting to match the snap_start_seqno the caller used when creating
     * the stream.
     * If firstMarkerSent is false this call will set it to true.
     * @param start a seqno we think should be the snapshot start
     * @return the snapshot start to use
     */
    uint64_t adjustStartIfFirstSnapshot(uint64_t start);

    //! Number of times a backfill is paused.
    cb::RelaxedAtomic<uint64_t> numBackfillPauses{0};

    /* The last sequence number queued from memory, but is yet to be
       snapshotted and put onto readyQ */
    std::atomic<uint64_t> lastReadSeqnoUnSnapshotted;

    //! The last sequence number sent to the network layer
    ATOMIC_MONOTONIC4(uint64_t, lastSentSeqno, Labeller, ThrowExceptionPolicy);

    //! The seqno of the last SeqnoAdvance sent
    ATOMIC_MONOTONIC3(uint64_t, lastSentSeqnoAdvance, Labeller);

    //! The last known seqno pointed to by the checkpoint cursor
    ATOMIC_WEAKLY_MONOTONIC3(uint64_t, curChkSeqno, Labeller);

    /**
     * Should the next snapshot marker have the 'checkpoint' flag
     * (MARKER_FLAG_CHK) set?
     * See comments in processItems() for usage of this variable.
     */
    bool nextSnapshotIsCheckpoint = false;

    /*
     * The next snapshot start that we will send (set when we set
     * nextSnapshotIsCheckpoint). Updated at the next checkpoint start which is
     * simpler than dealing with some transitions between backfill and memory
     * which also set nextSnapshotIsCheckpoint to true but do not need an
     * override snap start.
     */
    WEAKLY_MONOTONIC3(uint64_t, nextSnapStart, Labeller);

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
        std::atomic<size_t> bytes = 0;
        std::atomic<size_t> items = 0;
    } bufferedBackfill;

    /// Records the time at which the TakeoverSend phase begins.
    std::atomic<rel_time_t> takeoverStart = 0;

    /**
     * Maximum amount of time the TakeoverSend phase is permitted to take before
     * TakeoverSend is considered "backed up" and new frontend mutations will
     * paused.
     */
    const size_t takeoverSendMaxTime;
    //! Last snapshot start seqno sent to the DCP client, this value isn't used
    //! directly (apart from logging) but helps us to ensure that our
    //! snapshot start seqno we send are monotonic.
    ATOMIC_MONOTONIC3(uint64_t, lastSentSnapStartSeqno, Labeller);
    //! Last snapshot end seqno sent to the DCP client
    ATOMIC_MONOTONIC3(uint64_t, lastSentSnapEndSeqno, Labeller);

    /*
     * This is the highest seqno seen by KVStore when performing a scan
     * for the current snapshot we are back filling for. This is regardless of
     * collection or visibility. This used inform completeBackfill() of the last
     * read seqno from disk, to help make a decision on if we should enqueue an
     * SeqnoAdvanced op (see ::completeBackfill() for more info).
     */
    uint64_t maxScanSeqno{0};

    /* Flag used by checkpointCreatorTask that is set before all items are
       extracted for given checkpoint cursor, and is unset after all retrieved
       items are added to the readyQ */
    std::atomic<bool> chkptItemsExtractionInProgress;

    // Will the stream send dcp deletions with delete-times?
    const IncludeDeleteTime includeDeleteTime;

    /// Is PiTR enabled on this stream
    const PointInTimeEnabled pitrEnabled;

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
