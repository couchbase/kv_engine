/*
 *     Copyright 2025-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "dcp/backfill-manager.h"
#include "dcp/response.h"
#include "fuzz_test_helpers.h"
#include "tests/mock/mock_dcp_conn_map.h"
#include "tests/mock/mock_dcp_consumer.h"
#include "tests/module_tests/collections/collections_dcp_test.h"
#include "vbucket.h"

#include <folly/portability/GTest.h>
#include <fuzztest/fuzztest.h>
#include <fuzztest/googletest_fixture_adapter.h>
#include <logger/logger.h>

/**
 * Fuzz test for the PassiveStream class.
 * It uses the PerFuzzTestFixtureAdapter which initialises the fixture once and
 * reuses it across all test iterations. This avoids the need to re-create the
 * engine instance, but requires manual cleanup (reset() method).
 */
class PassiveStreamFuzzTest
    : public fuzztest::PerFuzzTestFixtureAdapter<CollectionsDcpTest> {
public:
    using CheckpointAction = cb::fuzzing::CheckpointAction;
    using CheckpointActionType = cb::fuzzing::CheckpointActionType;

    using Mutation = std::unique_ptr<MutationResponse>;

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

    void receiveSnapshot(SnapshotMarker marker,
                         std::vector<Mutation> mutations,
                         bool flushIfPersistent);

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

static void validatePurgeSeqno(uint64_t purgeSeqno,
                               bool isPersistent,
                               bool isFlushed,
                               bool isComplete,
                               bool isDiskSnapshot,
                               uint64_t highSeqno,
                               std::optional<uint64_t> markerPurgeSeqno) {
    if (isComplete) {
        if (isFlushed) {
            if (isDiskSnapshot) {
                EXPECT_EQ(purgeSeqno, markerPurgeSeqno.value_or(0));
            } else {
                EXPECT_EQ(purgeSeqno, 0);
            }
        } else {
            EXPECT_EQ(purgeSeqno, 0);
        }
    } else {
        if (isPersistent && isFlushed && markerPurgeSeqno.has_value() &&
            isDiskSnapshot) {
            // If we persist an incomplete snapshot, the purge seqno should
            // be the last seqno that was persisted.
            EXPECT_EQ(purgeSeqno,
                      std::min(markerPurgeSeqno.value(), highSeqno));
        } else {
            EXPECT_EQ(purgeSeqno, 0);
        }
    }
}

static void validateHighPreparedSeqno(
        uint64_t highPreparedSeqno,
        bool isComplete,
        bool isFlushed,
        bool isDiskSnapshot,
        std::optional<uint64_t> markerHighPreparedSeqno,
        uint64_t markerEndSeqno) {
    if (isComplete && isFlushed && isDiskSnapshot) {
        if (markerHighPreparedSeqno.has_value()) {
            EXPECT_EQ(highPreparedSeqno, markerHighPreparedSeqno.value());
        } else {
            EXPECT_EQ(highPreparedSeqno, markerEndSeqno);
        }
    } else {
        EXPECT_EQ(highPreparedSeqno, 0);
    }
}

void PassiveStreamFuzzTest::receiveSnapshot(
        const SnapshotMarker marker,
        const std::vector<Mutation> mutations,
        bool flushIfPersistent) {
    reset();

    auto& vb = *store->getVBucket(vbid);
    auto consumer = createConsumer();
    connMap->removeVBConnByVBId(consumer->getCookie(), vbid);

    auto stream = std::static_pointer_cast<MockPassiveStream>(
            consumer->makePassiveStream(
                    *engine,
                    consumer,
                    "test-passive-stream",
                    {} /* flags */,
                    0 /* opaque */,
                    vb,
                    0 /* startSeqno */,
                    0 /* vbUuid */,
                    0 /* snapStartSeqno */,
                    0 /* snapEndSeqno */,
                    0 /* vb_high_seqno */,
                    Collections::ManifestUid{} /* vb_manifest_uid */));
    stream->acceptStream(cb::mcbp::Status::Success, 0);
    stream->processMarker(const_cast<SnapshotMarker*>(&marker));

    // Do not send mutations beyond the snapshot end seqno.
    const auto maxMutations =
            std::min(mutations.size(),
                     size_t(marker.getEndSeqno() - marker.getStartSeqno() + 1));
    MutationResponse* lastMutation = nullptr;

    // Our counter for the MVS, used to validate the MVS is correct.
    uint64_t maxVisibleSeqno = vb.getMaxVisibleSeqno();
    // Count processed mutations, but dedup by key.
    std::unordered_set<std::string> processedKeys;

    for (size_t i = 0; i < maxMutations; i++) {
        if (!mutations[i]) {
            // Some seqnos are omitted (could be duplicates).
            continue;
        }
        auto& mutation = *mutations[i];
        mutation.getItem()->setBySeqno(marker.getStartSeqno() + i);
        auto result =
                stream->public_processMessage(&mutation, EnforceMemCheck::No);
        EXPECT_EQ(result.getError(), cb::engine_errc::success)
                << "at seqno " << mutation.getItem()->getBySeqno();
        lastMutation = &mutation;
        processedKeys.insert(mutation.getItem()->getKey().to_string());
        if (mutation.getItem()->isVisible()) {
            maxVisibleSeqno = mutation.getItem()->getBySeqno();
        }
    }

    // Ephemeral does not flush to disk.
    const bool isFlushed =
            processedKeys.size() > 0 && (!persistent() || flushIfPersistent);
    if (isFlushed) {
        flushVBucketToDiskIfPersistent(vbid, int(processedKeys.size()));
    }

    // Validate seqnos.
    if (lastMutation) {
        EXPECT_EQ(lastMutation->getItem()->getBySeqno(), vb.getHighSeqno());
    }

    const auto isComplete = uint64_t(vb.getHighSeqno()) == marker.getEndSeqno();
    const auto isDiskSnapshot =
            isFlagSet(marker.getFlags(), DcpSnapshotMarkerFlag::Disk);

    validatePurgeSeqno(vb.getPurgeSeqno(),
                       persistent(),
                       isFlushed,
                       isComplete,
                       isDiskSnapshot,
                       vb.getHighSeqno(),
                       marker.getPurgeSeqno());
    validateHighPreparedSeqno(vb.acquireStateLockAndGetHighPreparedSeqno(),
                              isComplete,
                              isFlushed,
                              isDiskSnapshot,
                              marker.getHighPreparedSeqno(),
                              marker.getEndSeqno());

    EXPECT_EQ(vb.getMaxVisibleSeqno(), maxVisibleSeqno);

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

static auto receiveSnapshotDomains() {
    return fuzztest::TupleOf(
            cb::fuzzing::snapshotMarker(),
            fuzztest::VectorOf(
                    fuzztest::UniquePtrOf(cb::fuzzing::mutationConsumerMessage(
                            /* defaultCollectionOnly */ true)))
                    .WithMaxSize(cb::fuzzing::snapshotMarkerMaxMutations),
            fuzztest::Arbitrary<bool>());
}

FUZZ_TEST_F(PersistentPassiveStreamFuzzTest, receiveSnapshot)
        .WithDomains(receiveSnapshotDomains());

class PersistentPassiveStreamFuzzTest_Counterexamples
    : public PersistentPassiveStreamFuzzTest {};
