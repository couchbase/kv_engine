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

#include "checkpoint.h"
#include "collections/vbucket_filter.h"
#include "dcp/stream.h"
#include "vbucket.h"

class ActiveStream : public Stream,
                     public std::enable_shared_from_this<ActiveStream> {
public:
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

    uint32_t setDead(end_stream_status_t status) override;

    void notifySeqnoAvailable(uint64_t seqno) override;

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

    bool isForceValueCompressionEnabled() const {
        return forceValueCompression == ForceValueCompression::Yes;
    }

    bool isSnappyEnabled() const {
        return snappyEnabled == SnappyEnabled::Yes;
    }

    void addStats(ADD_STAT add_stat, const void* c) override;

    void addTakeoverStats(ADD_STAT add_stat, const void* c, const VBucket& vb);

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

protected:
    /**
     * @param vb reference to the associated vbucket
     *
     * @return the outstanding items for the stream's checkpoint cursor.
     */
    virtual std::vector<queued_item> getOutstandingItems(VBucket& vb);

    // Given a set of queued items, create mutation responses for each item,
    // and pass onto the producer associated with this stream.
    void processItems(std::vector<queued_item>& items,
                      const LockHolder& streamMutex);

    bool nextCheckpointItem();

    std::unique_ptr<DcpResponse> nextQueuedItem();

    /**
     * @return a DcpResponse to represent the item. This will be either a
     *         MutationResponse or SystemEventProducerMessage.
     */
    std::unique_ptr<DcpResponse> makeResponseFromItem(const queued_item& item);

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

    /* backfillRemaining is a stat recording the amount of
     * items remaining to be read from disk.  It is an atomic
     * because otherwise the function incrBackfillRemaining
     * must acquire the streamMutex lock.
     */
    std::atomic<size_t> backfillRemaining;

    std::unique_ptr<DcpResponse> backfillPhase(std::lock_guard<std::mutex>& lh);

    Cursor cursor;

private:
    std::unique_ptr<DcpResponse> next(std::lock_guard<std::mutex>& lh);

    std::unique_ptr<DcpResponse> inMemoryPhase();

    std::unique_ptr<DcpResponse> takeoverSendPhase();

    std::unique_ptr<DcpResponse> takeoverWaitPhase();

    std::unique_ptr<DcpResponse> deadPhase();

    void snapshot(std::deque<std::unique_ptr<DcpResponse>>& snapshot,
                  bool mark);

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
    void removeCheckpointCursor();

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

    /* The last sequence number queued from memory, but is yet to be
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
    std::weak_ptr<DcpProducer> producerPtr;

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

    // Will the stream send dcp deletions with delete-times?
    IncludeDeleteTime includeDeleteTime;

    // Will the stream encode the CollectionID in the key?
    DocKeyEncodesCollectionId includeCollectionID;

    // Will the stream be able to output expiry opcodes?
    EnableExpiryOutput enableExpiryOutput;

    /// Is Snappy compression supported on this connection?
    SnappyEnabled snappyEnabled;

    /// Should items be forcefully compressed on this stream?
    ForceValueCompression forceValueCompression;

    /**
     * The filter the stream will use to decide which keys should be transmitted
     */
    Collections::VB::Filter filter;

    /**
     * A stream-ID which is defined if the producer is using enabled to allow
     * many streams-per-vbucket
     */
    DcpStreamId sid;
};
