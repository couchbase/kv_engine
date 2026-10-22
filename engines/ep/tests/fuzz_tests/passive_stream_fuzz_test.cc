/*
 *     Copyright 2025-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "bucket_fuzz_test_fixture.h"
#include "checkpoint_manager.h"
#include "dcp/backfill-manager.h"
#include "dcp/response.h"
#include "fuzz_test_helpers.h"
#include "tests/mock/mock_dcp_conn_map.h"
#include "tests/mock/mock_dcp_consumer.h"
#include "tests/module_tests/collections/collections_dcp_test.h"
#include "vbucket.h"

#include <cblogger/logger.h>
#include <folly/portability/GTest.h>
#include <fuzztest/fuzztest.h>
#include <limits>
#include <map>
#include <set>
#include <span>

/**
 * Fuzz test for the PassiveStream class.
 * It uses the BucketFuzzTestFixture which initialises the fixture once and
 * reuses it across all test iterations. This avoids the need to re-create the
 * engine instance, but requires manual cleanup (reset() method).
 */
class PassiveStreamFuzzTest
    : public cb::fuzzing::BucketFuzzTestFixture<CollectionsDcpTest> {
public:
    using CheckpointAction = cb::fuzzing::CheckpointAction;
    using CheckpointActionType = cb::fuzzing::CheckpointActionType;

    /**
     * A snapshot for the replica to receive.
     */
    struct SnapshotInput {
        SnapshotMarker marker;
        /// The action at each seqno from the snapshot start. A std::nullopt is
        /// a seqno without a message, as the active had deduplicated the item.
        /// An action the active could not have sent also leaves a gap.
        std::vector<std::optional<CheckpointAction>> actions;
        /// Whether to flush the snapshot to disk, for a persistent bucket.
        bool flushIfPersistent;
    };

    void SetUp() override {
        CollectionsDcpTest::SetUp();

        auto connMapPtr = std::make_unique<MockDcpConnMap>(*engine);
        connMap = connMapPtr.get();
        engine->setDcpConnMap(std::move(connMapPtr));
        setVBucketState(vbid, vbucket_state_replica);
    }

    /**
     * Resets any state set by the test iteration.
     */
    void reset();

    /**
     * Create a mock producer.
     */
    std::shared_ptr<MockDcpConsumer> createConsumer();

    /**
     * Creates a stream of the consumer for the vbucket, accepted by the
     * active.
     */
    std::shared_ptr<MockPassiveStream> createStream(
            const std::shared_ptr<MockDcpConsumer>& consumer,
            uint64_t startSeqno,
            uint64_t snapStartSeqno,
            uint64_t snapEndSeqno);

    /**
     * Receives the snapshots in order. Each snapshot after the first starts
     * after the previous one, and is only received if the previous one was
     * received to its end.
     */
    void receiveSnapshots(const std::vector<SnapshotInput>& snapshots);

    /**
     * Receives the snapshots in order, reconnecting before each snapshot after
     * the first, whether the previous one was received to its end or not. The
     * active resumes from the snapshot start the replica requests.
     */
    void receiveSnapshotsAfterReconnect(
            const std::vector<SnapshotInput>& snapshots);

private:
    enum class Reconnect : bool { No, Yes };

    void receive(const std::vector<SnapshotInput>& snapshots,
                 Reconnect reconnect);

    MockDcpConnMap* connMap;
};

void PassiveStreamFuzzTest::reset() {
    // Clears checkpoint and HT.
    EXPECT_TRUE(store->resetVBucket(vbid));
    // Run checkpoint destroyer and VBucket deletion tasks.
    runReadyTasks(TaskType::NonIO);
    runReadyTasks(TaskType::AuxIO);
    // Resets the task executor (clears any remaining tasks).
    for (auto& [taskId, _pair] : task_executor->getTaskLocator()) {
        task_executor->cancel(taskId, true);
    }
    task_executor->unregisterTaskable(engine->getTaskable(), true);
    task_executor->registerTaskable(engine->getTaskable());
}

std::shared_ptr<MockDcpConsumer> PassiveStreamFuzzTest::createConsumer() {
    auto consumer = std::make_shared<MockDcpConsumer>(
            *engine, cookie, "test_producer->test_consumer", "test_consumer");
    EXPECT_EQ(cb::engine_errc::success,
              consumer->addStream(0 /*opaque*/, vbid, {} /*flags*/));

    consumer->enableSyncReplication();
    consumer->enableFlatBuffersSystemEvents();

    return consumer;
}

std::shared_ptr<MockPassiveStream> PassiveStreamFuzzTest::createStream(
        const std::shared_ptr<MockDcpConsumer>& consumer,
        uint64_t startSeqno,
        uint64_t snapStartSeqno,
        uint64_t snapEndSeqno) {
    auto& vb = *store->getVBucket(vbid);
    auto stream = std::static_pointer_cast<MockPassiveStream>(
            consumer->makePassiveStream(
                    *engine,
                    consumer,
                    "test-passive-stream",
                    {} /* flags */,
                    0 /* opaque */,
                    vb,
                    startSeqno,
                    0 /* vbUuid */,
                    snapStartSeqno,
                    snapEndSeqno,
                    vb.getHighSeqno(),
                    Collections::ManifestUid{} /* vb_manifest_uid */,
                    std::nullopt /* cacheTransfer */));
    stream->acceptStream(cb::mcbp::Status::Success, 0);
    return stream;
}

namespace {

/// A key, and whether it is in the prepared namespace rather than the
/// committed one.
using NamespacedKey = std::pair<std::string, bool>;

/**
 * The stream request of a replica, as DcpConsumer builds it from the snapshot
 * info of its checkpoints.
 */
struct ReconnectRequest {
    uint64_t startSeqno;
    uint64_t snapStartSeqno;
    uint64_t snapEndSeqno;
};

/**
 * The messages a replica receives for a snapshot.
 */
class ReceivedSnapshot {
public:
    explicit ReceivedSnapshot(SnapshotMarker marker, bool flush)
        : marker(std::move(marker)), flush(flush) {
    }

    bool isDisk() const {
        return isFlagSet(marker.getFlags(), DcpSnapshotMarkerFlag::Disk);
    }

    bool isComplete() const {
        return lastSeqno == marker.getEndSeqno();
    }

    SnapshotMarker marker;
    /// The stream request of the reconnect before the snapshot, if any.
    std::optional<ReconnectRequest> reconnect;
    std::vector<std::unique_ptr<DcpResponse>> messages;
    /// Whether the snapshot is flushed once received.
    bool flush;
    /// The keys received.
    std::set<NamespacedKey> keys;
    /// The seqno of the last message, 0 if none.
    uint64_t lastSeqno = 0;
    /// The seqno of the last Prepare received.
    std::optional<uint64_t> highPreparedSeqno;
};

/**
 * What the replica received in the snapshots so far.
 */
struct ReceivedState {
    uint64_t lastSeqno = 0;
    uint64_t maxVisibleSeqno = 0;
    cb::fuzzing::InFlightPrepares inFlightPrepares;
    /// The seqno of the last Prepare, including one an Abort replaced.
    uint64_t lastPrepareSeqno = 0;
    uint64_t highCompletedSeqno = 0;
    /// The seqno of the last Commit or Abort, including one deduplicated.
    uint64_t lastCompletionSeqno = 0;
    /// The type of the last action for each key in the open checkpoint.
    std::map<NamespacedKey, cb::fuzzing::CheckpointActionType> checkpointKeys;
    /// The snapshot range of the open checkpoint.
    uint64_t checkpointStartSeqno = 0;
    uint64_t checkpointEndSeqno = 0;
    /// The highest HPS of a Disk marker, or of its end without one.
    uint64_t diskHighPreparedSeqno = 0;
    /// The highest purge seqno of a Disk marker.
    uint64_t diskPurgeSeqno = 0;
};

} // namespace

/**
 * Moves a marker to start after the previous snapshot. The active sets the
 * Checkpoint flag on a transition involving a Disk snapshot (MB-42780).
 */
static SnapshotMarker followSnapshot(const SnapshotMarker& previous,
                                     const SnapshotMarker& marker) {
    // The offset wraps when the marker moves back, which still adds up.
    // TODO: The HCS and purge seqno of a marker are at or after its start, but
    // an active may send them below it.
    const auto rebased = cb::fuzzing::rebaseSnapshotMarker(
            marker, previous.getEndSeqno() + 1 - marker.getStartSeqno());
    auto flags = rebased.getFlags();
    // TODO: Older actives did not set the Checkpoint flag on a transition
    // involving a Disk snapshot (MB-42780).
    if (isFlagSet(previous.getFlags(), DcpSnapshotMarkerFlag::Disk) ||
        isFlagSet(flags, DcpSnapshotMarkerFlag::Disk)) {
        flags |= DcpSnapshotMarkerFlag::Checkpoint;
    }
    return {rebased.getOpaque(),
            rebased.getVBucket(),
            rebased.getStartSeqno(),
            rebased.getEndSeqno(),
            flags,
            rebased.getHighCompletedSeqno(),
            rebased.getHighPreparedSeqno(),
            rebased.getMaxVisibleSeqno(),
            rebased.getPurgeSeqno(),
            rebased.getStreamId()};
}

/**
 * The stream request of a replica which reconnects. It resumes the snapshot
 * of the open checkpoint, unless it received the whole snapshot.
 */
static ReconnectRequest getReconnectRequest(const ReceivedState& state) {
    if (state.lastSeqno < state.checkpointStartSeqno ||
        state.lastSeqno == state.checkpointEndSeqno) {
        return {state.lastSeqno, state.lastSeqno, state.lastSeqno};
    }
    return {state.lastSeqno,
            state.checkpointStartSeqno,
            state.checkpointEndSeqno};
}

/**
 * Moves a marker to resume after the seqno the replica requested. The first
 * marker of a stream starts at the requested snapshot start, and a backfill
 * always has the Checkpoint flag.
 */
static SnapshotMarker resumeSnapshot(const ReconnectRequest& request,
                                     const SnapshotMarker& marker) {
    // The offset wraps when the marker moves back, which still adds up.
    // TODO: A resumed Memory marker ends at least where the checkpoint of the
    // active does, so not before the snapshot it resumes.
    const auto rebased = cb::fuzzing::rebaseSnapshotMarker(
            marker, request.startSeqno + 1 - marker.getStartSeqno());
    auto flags = rebased.getFlags();
    if (isFlagSet(flags, DcpSnapshotMarkerFlag::Disk)) {
        flags |= DcpSnapshotMarkerFlag::Checkpoint;
    }
    return {rebased.getOpaque(),
            rebased.getVBucket(),
            request.snapStartSeqno,
            rebased.getEndSeqno(),
            flags,
            rebased.getHighCompletedSeqno(),
            rebased.getHighPreparedSeqno(),
            rebased.getMaxVisibleSeqno(),
            rebased.getPurgeSeqno(),
            rebased.getStreamId()};
}

/**
 * Builds the messages the replica receives for a snapshot. Only messages an
 * active could send are built.
 *
 * @param marker The snapshot marker.
 * @param firstSeqno The seqno of the first action, the marker start unless the
 *        snapshot resumes after the seqno the replica requested.
 * @param input The snapshot generated by the fuzzer.
 * @param flush Whether to flush the snapshot once received.
 * @param state What the replica received so far, updated by the snapshot.
 * @return The snapshot, or std::nullopt if an active could not send it.
 */
static std::optional<ReceivedSnapshot> buildSnapshot(
        const SnapshotMarker& marker,
        uint64_t firstSeqno,
        const PassiveStreamFuzzTest::SnapshotInput& input,
        bool flush,
        ReceivedState& state) {
    using namespace cb::fuzzing;
    const auto isDiskSnapshot =
            isFlagSet(marker.getFlags(), DcpSnapshotMarkerFlag::Disk);
    auto& inFlightPrepares = state.inFlightPrepares;
    ReceivedSnapshot snapshot(marker, flush);
    // The seqnos without a message, where a deduplicated item may have been.
    std::vector<uint64_t> gaps;

    // Do not send messages beyond the snapshot end seqno.
    const auto maxActions = std::min(
            input.actions.size(),
            static_cast<size_t>(marker.getEndSeqno() - firstSeqno + 1));
    for (size_t ii = 0; ii < maxActions; ++ii) {
        const auto seqno = firstSeqno + ii;
        if (!input.actions[ii]) {
            gaps.push_back(seqno);
            continue;
        }
        const auto& action = *input.actions[ii];
        // In a Disk snapshot an Abort may have replaced its Prepare, which then
        // had one of the seqnos without a message. Prepares complete in order,
        // so it follows the last completed one and precedes those in flight.
        // TODO: The missing Prepare may also precede the snapshot (MB-44832).
        const bool abortsMissingPrepare =
                isDiskSnapshot && action.type == CheckpointActionType::Abort &&
                !inFlightPrepares.isInFlight(action.key) && !gaps.empty() &&
                gaps.back() > state.highCompletedSeqno &&
                inFlightPrepares.getOldestSeqno().value_or(seqno) > gaps.back();
        // In a Disk snapshot a Prepare may replace one received in an earlier
        // snapshot, the Abort of which had one of the seqnos without a message
        // (the duplicate prepare window). The replaced Prepare is the oldest in
        // flight, as Prepares complete in order.
        // TODO: In a Disk snapshot a later Prepare may complete before an
        // earlier one, as deduplication reorders completions (MB-36262,
        // MB-41300).
        const bool replacesPrepare =
                isDiskSnapshot &&
                action.type == CheckpointActionType::Prepare &&
                inFlightPrepares.canQueue(action.key,
                                          CheckpointActionType::Abort) &&
                inFlightPrepares.getOldestSeqno() < firstSeqno &&
                !gaps.empty() && gaps.back() > state.lastCompletionSeqno;
        const auto key = NamespacedKey(
                action.key.to_string(),
                action.type == CheckpointActionType::Prepare ||
                        action.type == CheckpointActionType::Abort);
        // Only Mutations deduplicate within a checkpoint. For anything else
        // the active opens a new checkpoint, so a new snapshot.
        // TODO: A backfill sends a single committed item per key, so Mutations
        // only deduplicate within a Memory checkpoint.
        const auto previous = state.checkpointKeys.find(key);
        if ((!abortsMissingPrepare && !replacesPrepare &&
             !inFlightPrepares.canQueue(action.key, action.type)) ||
            (previous != state.checkpointKeys.end() &&
             (previous->second != CheckpointActionType::Mutation ||
              action.type != CheckpointActionType::Mutation))) {
            gaps.push_back(seqno);
            continue;
        }
        uint64_t prepareSeqno = 0;
        switch (action.type) {
        case CheckpointActionType::Mutation:
            state.maxVisibleSeqno = seqno;
            break;
        case CheckpointActionType::Prepare:
            // TODO: Prepares are all Level::Majority. A PersistToMajority
            // Prepare remains tracked once completed, until persisted
            // (MB-35933, MB-36735).
            if (replacesPrepare) {
                state.highCompletedSeqno =
                        inFlightPrepares.complete(action.key);
                state.lastCompletionSeqno = gaps.back();
                gaps.pop_back();
            }
            inFlightPrepares.prepare(action.key, seqno);
            snapshot.highPreparedSeqno = seqno;
            state.lastPrepareSeqno = seqno;
            break;
        case CheckpointActionType::Commit:
            prepareSeqno = inFlightPrepares.complete(action.key);
            state.highCompletedSeqno = prepareSeqno;
            state.lastCompletionSeqno = seqno;
            state.maxVisibleSeqno = seqno;
            break;
        case CheckpointActionType::Abort:
            if (abortsMissingPrepare) {
                prepareSeqno = gaps.back();
                gaps.pop_back();
                state.lastPrepareSeqno =
                        std::max(state.lastPrepareSeqno, prepareSeqno);
            } else {
                prepareSeqno = inFlightPrepares.complete(action.key);
            }
            state.highCompletedSeqno = prepareSeqno;
            state.lastCompletionSeqno = seqno;
            break;
        case CheckpointActionType::CreateCheckpoint:
            throw std::logic_error("CreateCheckpoint is not received");
        }
        state.lastSeqno = seqno;
        snapshot.lastSeqno = seqno;
        state.checkpointKeys[key] = action.type;
        snapshot.keys.insert(key);
        // A backfill sends a Commit as a Mutation, which completes the Prepare
        // on a replica receiving a Disk snapshot.
        auto sentAction = action;
        if (isDiskSnapshot && action.type == CheckpointActionType::Commit) {
            sentAction.type = CheckpointActionType::Mutation;
        }
        snapshot.messages.push_back(
                createMessageForConsumer(sentAction, seqno, prepareSeqno));
    }

    // The HPS and HCS of a Disk marker cover the SyncWrites received, and
    // the HCS precedes the Prepares in flight, as they complete in order. The
    // HPS of the active does not move back.
    const auto markerHighPreparedSeqno =
            marker.getHighPreparedSeqno().value_or(marker.getEndSeqno());
    const auto markerHighCompletedSeqno =
            marker.getHighCompletedSeqno().value_or(0);
    if (isDiskSnapshot &&
        (markerHighPreparedSeqno < state.lastPrepareSeqno ||
         markerHighPreparedSeqno < state.diskHighPreparedSeqno ||
         markerHighCompletedSeqno < state.highCompletedSeqno ||
         markerHighCompletedSeqno >=
                 inFlightPrepares.getOldestSeqno().value_or(
                         std::numeric_limits<uint64_t>::max()))) {
        return std::nullopt;
    }
    if (isDiskSnapshot) {
        state.diskHighPreparedSeqno = markerHighPreparedSeqno;
        state.diskPurgeSeqno = std::max(state.diskPurgeSeqno,
                                        marker.getPurgeSeqno().value_or(0));
    }
    return snapshot;
}

/**
 * The number of items the flusher persists for the snapshots. It deduplicates
 * a key within each namespace of a batch, and only batches consecutive Memory
 * checkpoints together.
 */
static size_t getFlushCount(std::span<const ReceivedSnapshot> snapshots) {
    size_t count = 0;
    std::set<NamespacedKey> batch;
    bool isMemoryBatch = false;
    for (const auto& snapshot : snapshots) {
        if (!isMemoryBatch || snapshot.isDisk()) {
            count += batch.size();
            batch.clear();
        }
        batch.insert(snapshot.keys.begin(), snapshot.keys.end());
        isMemoryBatch = !snapshot.isDisk();
    }
    return count + batch.size();
}

/**
 * The purge seqno of a persistent vbucket once the snapshots are flushed. The
 * flusher applies the purge seqno of a Disk snapshot, capped to its last item.
 */
static uint64_t getFlushedPurgeSeqno(
        std::span<const ReceivedSnapshot> snapshots, uint64_t purgeSeqno) {
    for (const auto& snapshot : snapshots) {
        const auto markerPurgeSeqno = snapshot.marker.getPurgeSeqno();
        if (snapshot.isDisk() && snapshot.lastSeqno != 0 && markerPurgeSeqno) {
            purgeSeqno =
                    std::max(purgeSeqno,
                             std::min(*markerPurgeSeqno, snapshot.lastSeqno));
        }
    }
    return purgeSeqno;
}

/**
 * The HPS of the PDM, which moves at the end of each snapshot in order, and
 * at the end of a Disk snapshot only once it is persisted. A snapshot cut
 * short by a reconnect has no end, so its Prepares wait for the next one.
 *
 * @param persistedSnapshots The number of snapshots persisted.
 */
static uint64_t getHighPreparedSeqno(
        std::span<const ReceivedSnapshot> snapshots,
        size_t persistedSnapshots) {
    uint64_t highPreparedSeqno = 0;
    std::optional<uint64_t> receivedPreparedSeqno;
    for (size_t ii = 0; ii < snapshots.size(); ++ii) {
        const auto& snapshot = snapshots[ii];
        if (snapshot.highPreparedSeqno) {
            receivedPreparedSeqno = snapshot.highPreparedSeqno;
        }
        if (!snapshot.isComplete()) {
            if (ii + 1 < snapshots.size() && snapshots[ii + 1].reconnect) {
                continue;
            }
            break;
        }
        if (snapshot.isDisk()) {
            if (ii >= persistedSnapshots) {
                break;
            }
            highPreparedSeqno = snapshot.marker.getHighPreparedSeqno().value_or(
                    snapshot.marker.getEndSeqno());
        } else {
            highPreparedSeqno =
                    receivedPreparedSeqno.value_or(highPreparedSeqno);
        }
        receivedPreparedSeqno.reset();
    }
    return highPreparedSeqno;
}

/**
 * The HPS of an ephemeral SeqList, which moves with each Prepare received,
 * and to the HPS of the marker at the end of a Disk snapshot.
 */
static uint64_t getSeqListHighPreparedSeqno(
        std::span<const ReceivedSnapshot> snapshots) {
    uint64_t highPreparedSeqno = 0;
    for (const auto& snapshot : snapshots) {
        highPreparedSeqno =
                snapshot.highPreparedSeqno.value_or(highPreparedSeqno);
        if (snapshot.isDisk() && snapshot.isComplete()) {
            highPreparedSeqno = snapshot.marker.getHighPreparedSeqno().value_or(
                    snapshot.marker.getEndSeqno());
        }
    }
    return highPreparedSeqno;
}

void PassiveStreamFuzzTest::receiveSnapshots(
        const std::vector<SnapshotInput>& snapshots) {
    receive(snapshots, Reconnect::No);
}

void PassiveStreamFuzzTest::receiveSnapshotsAfterReconnect(
        const std::vector<SnapshotInput>& snapshots) {
    receive(snapshots, Reconnect::Yes);
}

void PassiveStreamFuzzTest::receive(const std::vector<SnapshotInput>& snapshots,
                                    Reconnect reconnect) try {
    std::vector<ReceivedSnapshot> received;
    ReceivedState state;
    for (const auto& input : snapshots) {
        std::optional<ReconnectRequest> request;
        auto marker = input.marker;
        if (!received.empty() && reconnect == Reconnect::Yes) {
            // TODO: Reconnect more than once (MB-34412), and receive an Abort
            // without its Prepare in a Memory snapshot after a reconnect
            // (MB-34474).
            request = getReconnectRequest(state);
            marker = resumeSnapshot(*request, input.marker);
            // The active rolls the replica back if it purged items after the
            // requested seqno, including those it already sent. The purge
            // seqno of the active does not move back.
            if (std::max(state.diskPurgeSeqno,
                         marker.getPurgeSeqno().value_or(0)) >
                request->startSeqno) {
                return;
            }
        } else if (!received.empty()) {
            if (!received.back().isComplete()) {
                // The active only moves on once it sent the whole snapshot.
                break;
            }
            marker = followSnapshot(received.back().marker, input.marker);
        }
        // The first marker of a stream creates a checkpoint, as does one with
        // the Checkpoint flag. Otherwise a Memory marker after a Memory one
        // extends the open checkpoint.
        if (received.empty() || request ||
            isFlagSet(marker.getFlags(), DcpSnapshotMarkerFlag::Checkpoint) ||
            isFlagSet(marker.getFlags(), DcpSnapshotMarkerFlag::Disk) ||
            received.back().isDisk()) {
            state.checkpointKeys.clear();
            state.checkpointStartSeqno = marker.getStartSeqno();
        }
        state.checkpointEndSeqno = marker.getEndSeqno();
        const auto firstSeqno =
                request ? request->startSeqno + 1 : marker.getStartSeqno();
        auto snapshot = buildSnapshot(marker,
                                      firstSeqno,
                                      input,
                                      persistent() && input.flushIfPersistent,
                                      state);
        if (!snapshot) {
            return;
        }
        snapshot->reconnect = request;
        received.push_back(std::move(*snapshot));
    }

    reset();

    auto& vb = *store->getVBucket(vbid);
    auto consumer = createConsumer();
    connMap->removeVBConnByVBId(consumer->getCookie(), vbid);

    auto stream = createStream(consumer,
                               0 /* startSeqno */,
                               0 /* snapStartSeqno */,
                               0 /* snapEndSeqno */);

    // Ephemeral does not flush to disk.
    size_t persistedSnapshots = persistent() ? 0 : received.size();
    uint64_t purgeSeqno = 0;
    for (size_t ii = 0; ii < received.size(); ++ii) {
        const auto& snapshot = received[ii];
        if (const auto& request = snapshot.reconnect) {
            // As DcpConsumer builds the stream request.
            auto info = vb.checkpointManager->getSnapshotInfo();
            if (info.range.getEnd() == info.start) {
                info.range.setStart(info.start);
            }
            EXPECT_EQ(request->startSeqno, info.start);
            EXPECT_EQ(request->snapStartSeqno, info.range.getStart());
            EXPECT_EQ(request->snapEndSeqno, info.range.getEnd());
            stream->setDead(cb::mcbp::DcpStreamEndStatus::Disconnected);
            stream = createStream(consumer,
                                  request->startSeqno,
                                  request->snapStartSeqno,
                                  request->snapEndSeqno);
        }
        stream->processMarker(snapshot.marker);
        for (const auto& message : snapshot.messages) {
            auto result = stream->public_processMessage(*message,
                                                        EnforceMemCheck::No);
            EXPECT_EQ(result.getError(), cb::engine_errc::success) << *message;
        }
        if (snapshot.flush) {
            // The flusher persists the snapshots received since the last flush.
            const auto flushed = std::span(received).subspan(
                    persistedSnapshots, ii + 1 - persistedSnapshots);
            const auto flushCount = getFlushCount(flushed);
            if (flushCount != 0) {
                flushVBucketToDiskIfPersistent(vbid,
                                               static_cast<int>(flushCount));
            }
            purgeSeqno = getFlushedPurgeSeqno(flushed, purgeSeqno);
            persistedSnapshots = ii + 1;
        }
    }

    // Validate seqnos.
    // TODO: Validate the item count (MB-50620).
    if (state.lastSeqno != 0) {
        EXPECT_EQ(state.lastSeqno, vb.getHighSeqno());
    }

    // TODO: MB-74678
    // An ephemeral replica does not apply the replicated purge seqno, which
    // is only picked up by the flusher.
    if (persistent()) {
        EXPECT_EQ(vb.getPurgeSeqno(), purgeSeqno);
    }
    // TODO: Validate the HPS a persistent vbucket persists (MB-51639,
    // MB-51806, MB-67632).
    EXPECT_EQ(vb.acquireStateLockAndGetHighPreparedSeqno(),
              getHighPreparedSeqno(received, persistedSnapshots));
    // An ephemeral vbucket keeps a second HPS in the SeqList, which is what a
    // promoted replica sends in its backfill markers (MB-74563).
    if (!persistent()) {
        EXPECT_EQ(vb.getPersistedHighPreparedSeqno(),
                  getSeqListHighPreparedSeqno(received));
    }

    // TODO: Validate the visible snapshot end of the checkpoints (MB-36948).
    EXPECT_EQ(vb.getMaxVisibleSeqno(), state.maxVisibleSeqno);

    // TODO(MB-66315): Validate HCS (MB-65251).
    // HCS is updated in the PDM only when SyncWrite is committed, but the
    // disk state is updated from the received marker.
    // We can promote to active or read the disk state here to get the HCS.
} catch (const std::exception&) {
    auto logger = cb::logger::get();
    for (const auto& input : snapshots) {
        logger->error("Marker: {}", input.marker);
        logger->error("Actions: {}", cb::fuzzing::formatActions(input.actions));
        logger->error("Flush: {}", input.flushIfPersistent);
    }
    throw;
}

class PersistentPassiveStreamFuzzTest : public PassiveStreamFuzzTest {
public:
    void SetUp() override {
        config_string += "bucket_type=persistent";
        PassiveStreamFuzzTest::SetUp();
    }
};

class EphemeralPassiveStreamFuzzTest : public PassiveStreamFuzzTest {
public:
    void SetUp() override {
        config_string += "bucket_type=ephemeral";
        PassiveStreamFuzzTest::SetUp();
    }
};

static auto snapshotInput() {
    return fuzztest::StructOf<PassiveStreamFuzzTest::SnapshotInput>(
            cb::fuzzing::snapshotMarker(),
            fuzztest::VectorOf(fuzztest::OptionalOf(cb::fuzzing::consumerAction(
                                       /* defaultCollectionOnly */ true)))
                    .WithMaxSize(cb::fuzzing::snapshotMarkerMaxMutations),
            fuzztest::Arbitrary<bool>());
}

static auto receiveSnapshotsDomain() {
    return fuzztest::VectorOf(snapshotInput()).WithMinSize(1).WithMaxSize(2);
}

static auto receiveSnapshotsAfterReconnectDomain() {
    return fuzztest::VectorOf(snapshotInput()).WithSize(2);
}

FUZZ_TEST_F(PersistentPassiveStreamFuzzTest, receiveSnapshots)
        .WithDomains(receiveSnapshotsDomain());

FUZZ_TEST_F(EphemeralPassiveStreamFuzzTest, receiveSnapshots)
        .WithDomains(receiveSnapshotsDomain());

FUZZ_TEST_F(PersistentPassiveStreamFuzzTest, receiveSnapshotsAfterReconnect)
        .WithDomains(receiveSnapshotsAfterReconnectDomain());

FUZZ_TEST_F(EphemeralPassiveStreamFuzzTest, receiveSnapshotsAfterReconnect)
        .WithDomains(receiveSnapshotsAfterReconnectDomain());

class PersistentPassiveStreamFuzzTest_Counterexamples
    : public PersistentPassiveStreamFuzzTest {};
