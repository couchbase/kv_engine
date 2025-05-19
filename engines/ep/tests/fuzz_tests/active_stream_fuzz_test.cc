/*
 *     Copyright 2025-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "checkpoint_manager.h"
#include "dcp/backfill-manager.h"
#include "dcp/response.h"
#include "failover-table.h"
#include "fuzz_test_helpers.h"
#include "programs/engine_testapp/mock_cookie.h"
#include "tests/mock/mock_dcp.h"
#include "tests/mock/mock_dcp_producer.h"
#include "tests/mock/mock_stream.h"
#include "tests/module_tests/evp_store_single_threaded_test.h"
#include "vbucket.h"
#include <folly/portability/GTest.h>
#include <fuzztest/fuzztest.h>
#include <fuzztest/googletest_fixture_adapter.h>
#include <logger/logger.h>

/**
 * Fuzz test for the ActiveStream class.
 * It uses the PerFuzzTestFixtureAdapter which initialises the fixture once and
 * reuses it across all test iterations. This avoids the need to re-create the
 * engine instance, but requires manual cleanup (reset() method).
 */
class ActiveStreamFuzzTest
    : public fuzztest::PerFuzzTestFixtureAdapter<SingleThreadedKVBucketTest> {
public:
    using CheckpointAction = cb::fuzzing::CheckpointAction;
    using CheckpointActionType = cb::fuzzing::CheckpointActionType;

    void SetUp() override {
        SingleThreadedKVBucketTest::SetUp();
        setVBucketState(vbid, vbucket_state_active);
    }

    /**
     * Resets any state set by the test iteration.
     * - hashtable
     * - checkpoint manager
     * - tasks
     */
    void reset();

    /**
     * Writes the checkpoint actions into the CM.
     */
    std::shared_ptr<CheckpointCursor> processCheckpointActions(
            const std::vector<CheckpointAction>& actions);

    /**
     * Create a mock producer.
     */
    std::shared_ptr<MockDcpProducer> createProducer(
            bool collectionsSupported, SyncReplication syncReplication);

    /**
     * Excersises the ActiveStream::processItems method, by accepting a list of
     * checkpoint actions and processing them.
     * The test is specifically interested in excersising various monotonicity
     * invariants on the ActiveStream class.
     * To exercise the filtering logic we need to allow for the following:
     * - Collections enabled/disable
     * - Collection filter
     * - SyncWrites
     * The test does not attempt to exercise the value modification logic when
     * queuing items (shouldModifyItem / makeResponseFromItem), so we fix
     * IncludeValue, IncludeXattrs and IncludeDeletedUserXattrs to No.
     */
    void backfillAndMemoryStream(
            const std::vector<CheckpointAction>& preActions,
            const std::vector<CheckpointAction>& postActions,
            uint64_t startSeqno,
            uint64_t snapshotStartSeqno,
            uint64_t snapshotEndSeqno,
            uint64_t expelSeqno,
            bool ensureBackfill,
            std::optional<CollectionID> collectionFilter,
            bool collectionsSupported,
            SyncReplication syncReplication);
};

void ActiveStreamFuzzTest::reset() {
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

    setCollections(cookie, cb::fuzzing::createManifest());
}

std::shared_ptr<CheckpointCursor>
ActiveStreamFuzzTest::processCheckpointActions(
        const std::vector<CheckpointAction>& actions) {
    auto& vb = *store->getVBucket(vbid);
    auto& cm = *vb.checkpointManager;

    // Register cursor with randomly generated name.
    auto cursor = cm.registerCursorBySeqno(
                            fmt::format("backup_cursor_{}", fmt::ptr(&actions)),
                            0,
                            CheckpointCursor::Droppable::No)
                          .takeCursor()
                          .lock();

    for (const auto& action : actions) {
        if (action.type == CheckpointActionType::CreateCheckpoint) {
            cm.createNewCheckpoint();
            action.bySeqno = cm.getHighSeqno();
            continue;
        }

        auto item = createItem(action.key, action.type);
        cm.queueDirty(item, GenerateBySeqno::Yes, GenerateCas::Yes, nullptr);
        action.bySeqno = item->getBySeqno();
    }

    return cursor;
}

std::shared_ptr<MockDcpProducer> ActiveStreamFuzzTest::createProducer(
        bool collectionsSupported, SyncReplication syncReplication) {
    auto producer =
            std::make_shared<MockDcpProducer>(*engine,
                                              cookie,
                                              "test_producer->test_consumer",
                                              cb::mcbp::DcpOpenFlag::Producer,
                                              false /*startTask*/);
    producer->setSyncReplication(syncReplication);
    cookie_to_mock_cookie(cookie)->setCollectionsSupport(collectionsSupported);
    producer->setNoopEnabled(MockDcpProducer::NoopMode::Enabled);

    producer->createCheckpointProcessorTask();
    producer->scheduleCheckpointProcessorTask();
    return producer;
}

void ActiveStreamFuzzTest::backfillAndMemoryStream(
        const std::vector<CheckpointAction>& preActions,
        const std::vector<CheckpointAction>& postActions,
        uint64_t startSeqno,
        uint64_t snapStartSeqno,
        uint64_t snapEndSeqno,
        uint64_t expelSeqno,
        bool ensureBackfill,
        std::optional<CollectionID> collectionFilter,
        bool collectionsSupported,
        SyncReplication syncReplication) try {
    using namespace cb::testing;
    using namespace cb::fuzzing;

    if (preActions.empty() || snapStartSeqno > snapEndSeqno ||
        snapStartSeqno > startSeqno || snapEndSeqno < startSeqno) {
        return;
    }

    if (syncReplication != SyncReplication::No && collectionFilter) {
        // SyncWrites with a collection filter is not used.
        return;
    }

    reset();

    auto& vb = *store->getVBucket(vbid);
    auto& cm = *vb.checkpointManager;

    auto backupCursor = processCheckpointActions(preActions);
    if (persistent()) {
        flushVBucket(vbid);
    }

    if (ensureBackfill) {
        cm.clear();
    }

    auto expelCursor =
            cm.registerCursorBySeqno(
                      fmt::format("expel_cursor"),
                      std::min<uint64_t>(cm.getHighSeqno(), expelSeqno),
                      CheckpointCursor::Droppable::No)
                    .takeCursor()
                    .lock();
    // Allow expelling to occur.
    EXPECT_TRUE(cm.removeCursor(*backupCursor));
    cm.expelUnreferencedCheckpointItems();

    auto producer = createProducer(collectionsSupported, syncReplication);

    std::shared_ptr<MockActiveStream> stream;
    try {
        uint64_t rollbackSeqno;
        stream = producer->mockStreamRequest(
                cb::mcbp::DcpAddStreamFlag::None,
                /* opaque */ 0,
                vbid,
                startSeqno,
                ~0,
                /* uuid */ vb.failovers->getLatestUUID(),
                snapStartSeqno,
                snapEndSeqno,
                &rollbackSeqno,
                /* callback */ mock_dcp_add_failover_log,
                /* json */ createJsonFilter(collectionFilter));
    } catch (const cb::engine_error& e) {
        // This is expected to happen with some inputs.
        return;
    }
    stream->setActive();

    EXPECT_TRUE(cm.removeCursor(*expelCursor));

    // Process the first set of mutations and drain the readyQ.
    driveActiveStream(producer, stream);

    auto postCursor = processCheckpointActions(postActions);
    EXPECT_TRUE(cm.removeCursor(*postCursor));

    // Process the second set of mutations and drain the readyQ.
    driveActiveStream(producer, stream);

    EXPECT_FALSE(store->isMemUsageAboveBackfillThreshold());
    EXPECT_EQ(cb::engine_errc::success, producer->closeStream(0, vbid));
} catch (const std::exception& e) {
    auto logger = cb::logger::get();
    logger->error("PreActions: {}", preActions);
    logger->error("PostActions: {}", postActions);
    throw;
}

class PersistentActiveStreamFuzzTest : public ActiveStreamFuzzTest {
public:
    void SetUp() override {
        config_string += "bucket_type=persistent";
        ActiveStreamFuzzTest::SetUp();
    }
};

class EphemeralActiveStreamFuzzTest : public ActiveStreamFuzzTest {
public:
    void SetUp() override {
        config_string += "bucket_type=ephemeral";
        ActiveStreamFuzzTest::SetUp();
    }
};

static auto backfillAndStreamDomains() {
    return fuzztest::TupleOf(
            fuzztest::VectorOf(cb::fuzzing::checkpointAction()).WithMaxSize(5),
            fuzztest::VectorOf(cb::fuzzing::checkpointAction()).WithMaxSize(2),
            /* startSeqno */ fuzztest::InRange<uint64_t>(0, 5),
            /* snapStartSeqno */ fuzztest::InRange<uint64_t>(0, 5),
            /* snapEndSeqno */ fuzztest::InRange<uint64_t>(0, 5),
            /* expelSeqno */ fuzztest::InRange<uint64_t>(0, 5),
            /* ensureBackfill */ fuzztest::Arbitrary<bool>(),
            fuzztest::OptionalOf(cb::fuzzing::collectionEntry()),
            /* collectionsSupported */ fuzztest::Arbitrary<bool>(),
            fuzztest::ElementOf({SyncReplication::No,
                                 SyncReplication::SyncWrites,
                                 SyncReplication::SyncReplication}));
}

FUZZ_TEST_F(PersistentActiveStreamFuzzTest, backfillAndMemoryStream)
        .WithDomains(backfillAndStreamDomains());

FUZZ_TEST_F(EphemeralActiveStreamFuzzTest, backfillAndMemoryStream)
        .WithDomains(backfillAndStreamDomains());

class PersistentActiveStreamFuzzTest_Counterexamples
    : public PersistentActiveStreamFuzzTest {};

/**
 * MB-66612: Monotonic exceptions in ActiveStream, when a collection-filtered
 * stream (vegetable:highSeqno:2) is resumed (startSeqno:3, snap:[2,4]) and
 * requires a backfill (seqno:3). The backfill finds nothing and sends a
 * SeqnoAdvance. Before the fix, the SeqnoAdvance uses the curChkSeqno which
 * is that of an meta item. The second set of checkpoint actions contains a
 * mutation which then takes that same seqno, which used to trigger a monotonic
 *
 */
TEST_F(PersistentActiveStreamFuzzTest_Counterexamples, MB_66612) {
    using namespace cb::testing;
    using namespace cb::fuzzing;
    backfillAndMemoryStream(
            {CheckpointAction{makeStoredDocKey("a", CollectionEntry::vegetable),
                              CheckpointActionType::Mutation},
             CheckpointAction{makeStoredDocKey("a", CollectionEntry::vegetable),
                              CheckpointActionType::Mutation},
             CheckpointAction{makeStoredDocKey("b", CollectionEntry::defaultC),
                              CheckpointActionType::Mutation}},
            {CheckpointAction{makeStoredDocKey("c", CollectionEntry::vegetable),
                              CheckpointActionType::Mutation}},
            /* startSeqno */ 3,
            /* snapStartSeqno */ 2,
            /* snapEndSeqno */ 4,
            /* expelSeqno */ 0,
            /* ensureBackfill */ true,
            CollectionEntry::vegetable,
            /* collectionsSupported */ true,
            SyncReplication::No);
}
