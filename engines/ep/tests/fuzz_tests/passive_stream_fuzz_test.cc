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

    using Mutation = std::unique_ptr<MutationResponse>;

    /**
     * A snapshot for the replica to receive.
     */
    struct SnapshotInput {
        SnapshotMarker marker;
        /// The mutation at each seqno from the snapshot start. A nullptr is a
        /// seqno without a message, as the active had deduplicated the item.
        std::vector<Mutation> mutations;
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

private:
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
    std::vector<MutationResponse*> messages;
    /// Whether the snapshot is flushed once received.
    bool flush;
    /// The keys received.
    std::set<std::string> keys;
    /// The seqno of the last message, 0 if none.
    uint64_t lastSeqno = 0;
};

/**
 * What the replica received in the snapshots so far.
 */
struct ReceivedState {
    uint64_t lastSeqno = 0;
    uint64_t maxVisibleSeqno = 0;
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
 * Builds the messages the replica receives for a snapshot.
 *
 * @param marker The snapshot marker.
 * @param input The snapshot generated by the fuzzer.
 * @param flush Whether to flush the snapshot once received.
 * @param state What the replica received so far, updated by the snapshot.
 */
static ReceivedSnapshot buildSnapshot(
        const SnapshotMarker& marker,
        const PassiveStreamFuzzTest::SnapshotInput& input,
        bool flush,
        ReceivedState& state) {
    ReceivedSnapshot snapshot(marker, flush);

    // Do not send mutations beyond the snapshot end seqno.
    const auto maxMutations =
            std::min(input.mutations.size(),
                     static_cast<size_t>(marker.getEndSeqno() -
                                         marker.getStartSeqno() + 1));
    for (size_t ii = 0; ii < maxMutations; ++ii) {
        if (!input.mutations[ii]) {
            // Some seqnos are omitted (could be duplicates).
            continue;
        }
        auto& mutation = *input.mutations[ii];
        const auto seqno = marker.getStartSeqno() + ii;
        mutation.getItem()->setBySeqno(seqno);
        if (mutation.getItem()->isVisible()) {
            state.maxVisibleSeqno = seqno;
        }
        state.lastSeqno = seqno;
        snapshot.lastSeqno = seqno;
        snapshot.keys.insert(mutation.getItem()->getKey().to_string());
        snapshot.messages.push_back(&mutation);
    }
    return snapshot;
}

/**
 * The number of items the flusher persists for the snapshots. It deduplicates
 * a key within a batch, and only batches consecutive Memory checkpoints
 * together.
 */
static size_t getFlushCount(std::span<const ReceivedSnapshot> snapshots) {
    size_t count = 0;
    std::set<std::string> batch;
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
 * at the end of a Disk snapshot only once it is persisted.
 *
 * @param persistedSnapshots The number of snapshots persisted.
 */
static uint64_t getHighPreparedSeqno(
        std::span<const ReceivedSnapshot> snapshots,
        size_t persistedSnapshots) {
    uint64_t highPreparedSeqno = 0;
    for (size_t ii = 0; ii < snapshots.size(); ++ii) {
        const auto& snapshot = snapshots[ii];
        if (!snapshot.isComplete()) {
            break;
        }
        if (snapshot.isDisk()) {
            if (ii >= persistedSnapshots) {
                break;
            }
            highPreparedSeqno = snapshot.marker.getHighPreparedSeqno().value_or(
                    snapshot.marker.getEndSeqno());
        }
    }
    return highPreparedSeqno;
}

/**
 * The HPS of an ephemeral SeqList, which moves to the HPS of the marker at the
 * end of a Disk snapshot.
 */
static uint64_t getSeqListHighPreparedSeqno(
        std::span<const ReceivedSnapshot> snapshots) {
    uint64_t highPreparedSeqno = 0;
    for (const auto& snapshot : snapshots) {
        if (snapshot.isDisk() && snapshot.isComplete()) {
            highPreparedSeqno = snapshot.marker.getHighPreparedSeqno().value_or(
                    snapshot.marker.getEndSeqno());
        }
    }
    return highPreparedSeqno;
}

void PassiveStreamFuzzTest::receiveSnapshots(
        const std::vector<SnapshotInput>& snapshots) {
    std::vector<ReceivedSnapshot> received;
    ReceivedState state;
    for (const auto& input : snapshots) {
        auto marker = input.marker;
        if (!received.empty()) {
            if (!received.back().isComplete()) {
                // The active only moves on once it sent the whole snapshot.
                break;
            }
            marker = followSnapshot(received.back().marker, input.marker);
        }
        received.push_back(buildSnapshot(
                marker, input, persistent() && input.flushIfPersistent, state));
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
        stream->processMarker(snapshot.marker);
        for (auto* message : snapshot.messages) {
            auto result = stream->public_processMessage(*message,
                                                        EnforceMemCheck::No);
            EXPECT_EQ(result.getError(), cb::engine_errc::success)
                    << "at seqno " << message->getItem()->getBySeqno();
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

    // TODO(MB-66315): Validate HCS.
    // HCS is updated in the PDM only when SyncWrite is committed, but the
    // disk state is updated from the received marker.
    // We can promote to active or read the disk state here to get the HCS.
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
            fuzztest::VectorOf(
                    fuzztest::UniquePtrOf(cb::fuzzing::mutationConsumerMessage(
                            /* defaultCollectionOnly */
                            true)))
                    .WithMaxSize(cb::fuzzing::snapshotMarkerMaxMutations),
            fuzztest::Arbitrary<bool>());
}

static auto receiveSnapshotsDomain() {
    return fuzztest::VectorOf(snapshotInput()).WithMinSize(1).WithMaxSize(2);
}

FUZZ_TEST_F(PersistentPassiveStreamFuzzTest, receiveSnapshots)
        .WithDomains(receiveSnapshotsDomain());

FUZZ_TEST_F(EphemeralPassiveStreamFuzzTest, receiveSnapshots)
        .WithDomains(receiveSnapshotsDomain());

class PersistentPassiveStreamFuzzTest_Counterexamples
    : public PersistentPassiveStreamFuzzTest {};
