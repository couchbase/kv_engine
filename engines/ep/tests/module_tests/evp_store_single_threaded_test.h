/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2016-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

/*
 * Unit tests for the EPBucket class.
 */

#pragma once

#include "config.h"
#include "dcp/dcp-types.h"
#include "hash_table.h"
#include "kv_bucket_test.h"
#include "vbucket_queue_item_ctx.h"

#include <executor/fake_executorpool.h>
#include <libcouchstore/couch_db.h>
#include <memcached/protocol_binary.h>
#include <memcached/storeddockey_fwd.h>
#include <nlohmann/json.hpp>
#include <stdexcept>

class CollectionsManifest;
struct DcpMessageProducersIface;
class EPBucket;
class MockActiveStreamWithOverloadedRegisterCursor;
class MockDcpMessageProducers;
class MockDcpProducer;
struct failover_entry_t;

/*
 * A subclass of KVBucketTest which uses a fake ExecutorPool,
 * which will not spawn ExecutorThreads and hence not run any tasks
 * automatically in the background. All tasks must be manually run().
 */
class SingleThreadedKVBucketTest : public KVBucketTest {
public:
    /**
     * Run the next task from the taskQ
     *
     * @param expectedTaskName The expected name of the next task to be run.
     *        Will throw is this does not match the name of the actual next
     *        task.
     * @param timeAdvance Optional duration to advance the scheduler clock by,
     *        to allow tasks which are only scheduled to run at some future
     *        time to run "now", without having to actually wait for real-time
     *        to pass.
     */
    cb::time::steady_clock::time_point runNextTask(
            TaskQueue& taskQ,
            std::string_view expectedTaskName,
            cb::time::steady_clock::duration timeAdvance =
                    cb::time::steady_clock::duration::zero());

    /*
     * Run the next task from the taskQ
     */
    cb::time::steady_clock::time_point runNextTask(TaskQueue& taskQ);

    void runNextTask(TaskType t, std::string_view expectedTaskName);

    /*
     * Run all ready tasks of the given type
     * @return the wake time of the next task
     */
    cb::time::steady_clock::time_point runReadyTasks(TaskType t);

    /*
     * DCP helper. Create a MockDcpProducer configured with (or without)
     * collections and/or delete_times enabled
     * @param cookie cookie to associate with the new producer
     * @param deleteTime yes/no - enable/disable delete times
     * @param flatBuffersSystemEvents enable/disable producer with FlatBuffers
     *        system events
     */
    std::shared_ptr<MockDcpProducer> createDcpProducer(
            CookieIface* cookie,
            IncludeDeleteTime deleteTime,
            bool flatBuffersSystemEvents = true,
            const std::string name = "test_producer");

    /*
     * DCP helper.
     * Notify the given producer and ensure the checkpoint is ready for stepping
     * @param fromMemory if false then step a backfill
     */
    void notifyAndRunToCheckpoint(MockDcpProducer& producer,
                                  MockDcpMessageProducers& producers,
                                  bool fromMemory = true);

    /*
     * DCP helper.
     * Notify and step the given producer
     * @param expectedOp once stepped we expect to see this DCP opcode produced
     * @param fromMemory if false then step a backfill
     * @param diskSnapshotFromMemory in some edge cases we may sent a disk
     * snapshot from memory, if this pram is true we will run the checkpoint
     * task but check for the disk marker flag
     */
    void notifyAndStepToCheckpoint(
            MockDcpProducer& producer,
            MockDcpMessageProducers& producers,
            cb::mcbp::ClientOpcode expectedOp =
                    cb::mcbp::ClientOpcode::DcpSnapshotMarker,
            bool fromMemory = true,
            bool diskSnapshotFromMemory = false);

    /*
     * DCP helper.
     * Run the active-checkpoint processor task for the given producer
     * @param producer The producer whose task will be ran
     * @param producers The dcp callbacks
     */
    void runCheckpointProcessor(MockDcpProducer& producer,
                                DcpMessageProducersIface& producers);

    /*
     * DCP helper - Run the backfill tasks
     */
    void runBackfill();

    /**
     * Create a DCP stream on the producer for this->vbid
     */
    void createDcpStream(MockDcpProducer& producer);

    /**
     * Create a DCP stream on the producer for vbid
     */
    void createDcpStream(
            MockDcpProducer& producer,
            Vbid vbid,
            std::optional<std::string_view> streamConfig = std::nullopt);

    /**
     * Schedule and run the compaction task
     * @param id vbucket to compact
     * @param purgeBeforeSeq purge tombstones with seqnos less than this
     * @param dropDeletes drop all deletes
     */
    void runCompaction(Vbid id,
                       uint64_t purgeBeforeSeq = 0,
                       bool dropDeletes = false);

    /**
     * Schedule and run the task responsible for iterating the documents and
     * erasing them.
     *
     * For persistent buckets integrated into compaction.
     * For ephemeral buckets integrated into stale item removal task
     *
     * @param id vbucket to process
     * @param expectSuccess is the compaction supposed to be successful?
     */
    void scheduleAndRunCollectionsEraser(Vbid id, bool expectSuccess = true);

    /**
     * Run the task responsible for iterating the documents and erasing them.
     * Throws if we have not queued something in the write queue.
     *
     * For persistent buckets integrated into compaction.
     * For ephemeral buckets integrated into stale item removal task
     *
     * @param id vbucket to process
     */
    void runCollectionsEraser(Vbid id, bool expectSuccess = true);

    /**
     * Run the task responsible for destroying Checkpoints after they have
     * been removed from a CheckpointManager.
     */
    void runCheckpointDestroyer(Vbid id);

    bool isBloomFilterEnabled() const;

    bool isFullEviction() const;

    bool isPersistent() const;

    /**
     * Used to detect magma tests
     *
     * @returns true if bucket has magma via either magma backend or nexus
     */
    bool hasMagma() const {
        // Catch backend=magma and nexus_..._backend=magma
        return config_string.find("backend=magma") != std::string::npos;
    }

    bool isNexus() const;

    /// @returns true if this is a magma bucket
    bool isMagma() const;

    bool isCouchstore() const;

    bool isNexusMagmaPrimary() const;

    bool needsBGFetch(cb::engine_errc ec) const {
        if (ec == cb::engine_errc::would_block && isPersistent() &&
            isFullEviction()) {
            return true;
        }
        return false;
    }

    get_options_t needsBGFetchQueued() const {
        get_options_t ops = {};
        if (!isBloomFilterEnabled()) {
            ops = QUEUE_BG_FETCH;
        }
        return ops;
    }

    /**
     * Replaces the rw store for shard 0 with a MockCouchKVStore.
     */
    void replaceCouchKVStoreWithMock();

    /**
     * Set the collections manifest using the engine API (and drive any tasks)
     * @param cookie a cookie is needed for i/o callback
     * @param manifest the CollectionsManifest to set
     * @param status1 the first call to set_collection_manifest expected result
     *        usually would_block
     */
    cb::engine_errc setCollections(
            CookieIface* cookie,
            const CollectionsManifest& manifest,
            cb::engine_errc status1 = cb::engine_errc::would_block);

    /// @return the size of the future queue for the given task type
    size_t getFutureQueueSize(TaskType type) const;

    /// @return the size of the ready queue for the given task type
    size_t getReadyQueueSize(TaskType type) const;

    /*
     * Set the stats isShutdown and attempt to drive all tasks to cancel for
     * the specified engine.
     */
    void shutdownAndPurgeTasks(EventuallyPersistentEngine* ep);

    enum class VbucketOp : uint8_t { Set, Add };

    /**
     * Load documents to enter a TempOOM phase.
     *
     * @param op The MCBP opearation to use for the load
     * @return the num of items loaded
     */
    size_t loadUpToOOM(VbucketOp op);

    /**
     * Verifies that CM OOM prevents expirations from being processed and queued
     * into the CM.
     *
     * @param expiryFunc The logic that attempts docs expiration
     */
    void testExpiryObservesCMQuota(std::function<void()> expiryFunc);

    /**
     * Return the memory usage for the specified memory domain.
     */
    size_t getDomainMemoryAllocated(cb::MemoryDomain domain);

protected:
    void SetUp() override;

    void TearDown() override;

    /**
     * Set a new vbucket state in memory and queues a set-vbstate itam into the
     * CheckpointManager. But, this doesn't run the flusher, so the new state
     * isn't persisted.
     *
     * @param vbid
     * @param newState
     * @param meta Optional meta information to apply alongside the state
     * @param transfer Should vBucket be transferred without adding failover
     *                 table entry (i.e. takeover)?
     */
    void setVBucketState(Vbid vbid,
                         vbucket_state_t newState,
                         const nlohmann::json& meta = {},
                         TransferVB transfer = TransferVB::No);

    /**
     * Change the vbucket state, and run the VBStatePeristTask (if necessary
     * for this bucket type).
     * On return the state will be changed and the task completed.
     *
     * @param vbid
     * @param newState
     * @param meta Optional meta information to apply alongside the state
     * @param transfer Should vBucket be transferred without adding failover
     *                 table entry (i.e. takeover)?
     */
    void setVBucketStateAndRunPersistTask(Vbid vbid,
                                          vbucket_state_t newState,
                                          const nlohmann::json& meta = {},
                                          TransferVB transfer = TransferVB::No);

    void setVBucketToActiveWithValidTopology(
            nlohmann::json topology = nlohmann::json::array({{"active",
                                                              "replica"}}));

    void cancelAndPurgeTasks();

    /**
     * This method will keep running reader tasks until all Warmup stages warmup
     * is complete.
     */
    void runReadersUntilWarmedUp();

    /**
     * This method will keep running reader tasks until primary Warmup shows it
     * is complete
     */
    void runReadersUntilPrimaryWarmedUp();

    /**
     * This method will keep running reader tasks until secondary Warmup shows
     * it is complete
     */
    void runReadersUntilSecondaryWarmedUp();

    /**
     * Helper method that takes the objects current base config, will re-enable
     * warmup in the returned config and also add the new_config arg.
     * @param new_config args to add to the config
     * @return a config string with an warmup=true
     */
    std::string buildNewWarmupConfig(std::string new_config);

    /**
     * Destroy engine and replace it with a new engine.
     *
     * @param new_config The config to supply to engine creation
     * @param unclean Should the restart be made to appear unclean
     * @param encryptionKeys Boot-strap encryption keys to use for the new
     * engine
     */
    void resetEngine(std::string new_config = "",
                     bool unclean = false,
                     nlohmann::json encryptionKeys = nlohmann::json::object());

    /**
     * Destroy engine and replace it with a new engine that can be warmed up.
     *
     * @param new_config The config to supply to engine creation
     * @param unclean Should the restart be made to appear unclean
     * @param encryptionKeys Boot-strap encryption keys to use for the new
     * engine
     */
    void resetEngineAndEnableWarmup(
            std::string new_config = "",
            bool unclean = false,
            nlohmann::json encryptionKeys = nlohmann::json::object());

    /**
     * Destroy engine and replace it with a new engine that can be warmed up.
     * Finally, run warmup.
     *
     * @param new_config The config to supply to engine creation
     * @param unclean Should the restart be made to appear unclean
     * @param encryptionKeys Boot-strap encryption keys to use for the new
     * engine
     */
    void resetEngineAndWarmup(
            std::string new_config = "",
            bool unclean = false,
            nlohmann::json encryptionKeys = nlohmann::json::object());

    /*
     * Fake callback emulating dcp_add_failover_log
     */
    static cb::engine_errc fakeDcpAddFailoverLog(
            const std::vector<vbucket_failover_t>&) {
        return cb::engine_errc::success;
    }

    /**
     * Purge all tombstones before the specified seqno, advancing the
     * purgeSeqno to that seqno.
     */
    void purgeTombstonesBefore(uint64_t purgeSeqno);

    /**
     * Run the HTCleaner for Ephemeral bucket.
     *
     * @throws std::bad_cast If the underlying bucket is not Ephemeral.
     */
    void runEphemeralHTCleaner();

    /**
     * Get the latest failover table entry for vbucket with vbid
     * @return latest failover table or if no vbucket for vbid a default value
     *         failover_entry_t
     */
    std::optional<failover_entry_t> getLatestFailoverTableEntry() const;

    SingleThreadedExecutorPool* task_executor;
};

/**
 * Test fixture for single-threaded tests on EPBucket.
 */
class SingleThreadedEPBucketTest : public SingleThreadedKVBucketTest {
public:
    enum class BackfillBufferLimit { StreamByte, StreamItem, ConnectionByte };

    void producerReadyQLimitOnBackfill(BackfillBufferLimit limitType);

protected:
    EPBucket& getEPBucket();
};

/**
 * Test fixture for KVBucket tests running in single-threaded mode, for some
 * combination of bucket type, eviction mode and KVStore type.
 *
 * Allows tests to be defined once which are applicable to more than one
 * configuration, and then instantiated with appropriate config parameters.
 *
 * Currently parameterised on a config string which sets:
 * - bucket type (ephemeral or persistent, and additional persistent variants
 *   (e.g. Magma) for additional storage backends.
 * - eviction type.
 *   - For ephemeral buckets: used for specifying ephemeral auto-delete /
 *     fail_new_data
 *   - For persistent buckets: used for specifying value_only or full_eviction
 *
 * GTest does not like having ';'s in the config string, it emits another set of
 * empty test suites that ctest runs so we use ':' instead and replace it later.
 *
 * See `allConfigValues(), persistentConfigValues(), etc methods to instantiate
 * tests for some set / subset of the avbove parameters.
 *
 * Note that specific instantiations of tests may not instantiate for all
 * possible variants - a test may only be applicable to persistent buckets and
 * hence will only instantiate for persistentConfigValues.
 *
 * Suggested usage:
 * 1. For a given group of tests (e.g. CollectionsDCP tests), create a subclass
 *   of this class:
 *
 *     class MyTestSuite : public STParameterizedBucketTest {};
 *
 * 2. Write some (parameterized) tests:
 *
 *     TEST_P(MyTestSuite, DoesFoo) { ... }
 *
 * 3. Instantiate your test suite with the config values applicable to it -
 * for example a test which is applicable to all variants of a Persistent
 * bucket:
 *
 *     INSTANTIATE_TEST_SUITE_P(
 *         Persistent,
 *         MyTestSuite,
 *         STParameterizedBucketTest::persistentConfigValues(),
 *         STParameterizedBucketTest::PrintToStringParamName);
 *
 * Advanced usage:
 * - If you have some tests in a suite which only work for some config params
 *   but not others (e.g. some don't work under Ephemeral), split your suite
 *   into two sibling classes then instantiate each class with a different
 *   config:
 *
 *   class DcpActiveStreamTest : public STParameterizedBucketTest {};
 *   class DcpActiveStreamTestPersistent : public STParameterizedBucketTest {};
 *
 *   ... define some TEST_P() for each suite...
 *
 *     INSTANTIATE_TEST_SUITE_P(
 *         PersistentAndEphemeral,
 *         DcpActiveStreamTest,
 *         STParameterizedBucketTest::allConfigValues(),
 *         STParameterizedBucketTest::PrintToStringParamName);
 *
 *     INSTANTIATE_TEST_SUITE_P(
 *         Persistent,
 *         DcpActiveStreamTestPersistent,
 *         STParameterizedBucketTest::persistentAllBackendsConfigValues(),
 *         STParameterizedBucketTest::PrintToStringParamName());
 */
class STParameterizedBucketTest
    : virtual public SingleThreadedKVBucketTest,
      public ::testing::WithParamInterface<std::string> {
public:
    static auto persistentBucket() {
        return config::Config{{"bucket_type", "persistent"}};
    }

    static auto ephemeralBucket() {
        return config::Config{{"bucket_type", "ephemeral"}};
    }

    static auto ephAutoDelete() {
        return config::Config{{"ephemeral_full_policy", "auto_delete"}};
    }

    static auto ephFailNewData() {
        return config::Config{{"ephemeral_full_policy", "fail_new_data"}};
    }

    static auto ephFullPolicy() {
        return ephAutoDelete() | ephFailNewData();
    }

    static config::Config ephemeralMemRecoveryConfigValues() {
        return config::Config{
                {"ephemeral_mem_recovery_enabled", {"true", "false"}}};
    }

    static auto couchstoreBucket() {
        return persistentBucket() * config::Config{{"backend", "couchstore"}};
    }

    static auto magmaBucket() {
        return persistentBucket() * config::Config{{"backend", "magma"}};
    }

    static auto valueOnlyEvictionPolicy() {
        return persistentBucket() *
               config::Config{{"item_eviction_policy", "value_only"}};
    }

    static auto fullEvictionPolicy() {
        return persistentBucket() *
               config::Config{{"item_eviction_policy", "full_eviction"}};
    }

    static auto itemEvictionPolicy() {
        return valueOnlyEvictionPolicy() | fullEvictionPolicy();
    }

    static auto ephConfigValues() {
        return ephemeralBucket() * ephFullPolicy() *
               ephemeralMemRecoveryConfigValues();
    }

    static auto ephAutoDeleteConfigValues() {
        using namespace std::string_literals;
        return ephemeralBucket() * ephAutoDelete() *
               ephemeralMemRecoveryConfigValues();
    }

    static auto allConfigValues() {
#ifdef EP_USE_MAGMA
        return allConfigValuesNoNexus() | nexusCouchstoreMagmaConfigValues();
#else
        return allConfigValuesNoNexus();
#endif
    }

    static config::Config allConfigValuesNoNexus() {
        return ephConfigValues() | persistentNoNexusConfigValues();
    }

    static auto ephAndCouchstoreConfigValues() {
        return ephConfigValues() | couchstoreConfigValues();
    }

    static config::Config persistentConfigValues() {
#ifdef EP_USE_MAGMA
        return persistentNoNexusConfigValues() |
               nexusCouchstoreMagmaConfigValues();
#else
        return persistentNoNexusConfigValues();
#endif
    }

    static config::Config persistentNoNexusConfigValues() {
        auto configs = couchstoreConfigValues();
#ifdef EP_USE_MAGMA
        configs |= magmaConfigValues();
#endif
        return configs;
    }

    static config::Config couchstoreConfigValues() {
        return couchstoreBucket() * itemEvictionPolicy();
    }

#ifdef EP_USE_MAGMA
    static config::Config magmaConfigValues() {
        return magmaBucket() * itemEvictionPolicy() *
               magmaPerDocumentCompressionConfigValues();
    }

    static auto nexusCouchstoreMagma() {
        return persistentBucket() *
               config::Config{{"backend", "nexus"},
                              {"nexus_primary_backend", "couchstore"},
                              { "nexus_secondary_backend",
                                "magma" }};
    }

    static auto nexusMagmaCouchstore() {
        return persistentBucket() *
               config::Config{{"backend", "nexus"},
                              {"nexus_primary_backend", "magma"},
                              { "nexus_secondary_backend",
                                "couchstore" }};
    }

    static config::Config nexusCouchstoreMagmaConfigValues() {
        return nexusCouchstoreMagma() * itemEvictionPolicy();
    }

    static auto nexusCouchstoreMagmaAllConfigValues() {
        return (nexusCouchstoreMagma() | nexusMagmaCouchstore()) *
               itemEvictionPolicy();
    }

    static config::Config magmaPerDocumentCompressionConfigValues() {
        return config::Config{
                { "magma_per_document_compression_enabled",
                  { "true",
                    "false" } }};
    }

#endif

    static auto persistentAllBackendsConfigValues() {
        auto configs = couchstoreConfigValues();
#ifdef EP_USE_MAGMA
        configs |= nexusCouchstoreMagmaAllConfigValues();
        configs |= magmaConfigValues();
#endif
        return configs;
    }

    static auto persistentAllBackendsNoNexusConfigValues() {
        auto configs = couchstoreConfigValues();
#ifdef EP_USE_MAGMA
        configs |= magmaConfigValues();
#endif
        return configs;
    }

    static auto fullEvictionAllBackendsConfigValues() {
        using namespace std::string_literals;
        auto configs = couchstoreBucket();
#ifdef EP_USE_MAGMA
        configs |= magmaBucket();
        configs |= nexusCouchstoreMagma();
#endif
        return configs * fullEvictionPolicy();
    }

    bool persistent() const {
        return GetParam().find("persistent") != std::string::npos;
    }

    bool ephemeral() const {
        return GetParam().find("ephemeral") != std::string::npos;
    }

    bool ephemeralFailNewData() const;

    bool isEphemeralMemRecoveryEnabled() const;

    bool fullEviction() const;

    /// @returns true if this is a magma bucket
    bool isMagma() const;

    /**
     * Check if Snappy compression will be applied at persistence
     */
    bool isSnappyCompressedAtPersistence() const;

    bool isNexusMagmaPrimary() const;

    bool isNexus() const;

    std::string getBackendString() const {
        return GetParam();
    }

    bool bloomFilterEnabled() const;

    /// @returns a string representing this tests' parameters.
    static std::string PrintToStringParamName(
            const ::testing::TestParamInfo<ParamType>& info);

    /**
     * This function is for handling cases where we get an EWOULDBLOCK
     * error so we trigger a BGFetch. This can happen when bloom
     * filters are turned off, for instance for magma.
     *
     * @param rc cb::engine_errc returned from attempted op
     * @return true if did BGFetch
     */
    bool needBGFetch(cb::engine_errc rc) {
        if (rc == cb::engine_errc::would_block && persistent() &&
            fullEviction()) {
            runBGFetcherTask();
            return true;
        }
        return false;
    }

    /**
     * Check to see if Key Exists.
     * Handles case when we get cb::engine_errc::would_block.
     *
     * @param key doc key
     * @param vbid vbucket id
     * @param options fetch options
     * @return cb::engine_errc return status of get call
     */
    cb::engine_errc checkKeyExists(StoredDocKey& key,
                                   Vbid vbid,
                                   get_options_t options);

    /**
     * Call kvstore SET.
     * Handles case when we get cb::engine_errc::would_block.
     *
     * @param item item to be SET
     * @param cookie mock cookie
     * @return cb::engine_errc return status of SET call
     */
    cb::engine_errc setItem(Item& itm, CookieIface* cookie);

    /**
     * Call kvstore ADD.
     * Handles case when we get cb::engine_errc::would_block.
     *
     * @param item item to be ADD
     * @param cookie mock cookie
     * @return cb::engine_errc return status of ADD call
     */
    cb::engine_errc addItem(Item& itm, CookieIface* cookie);

    /**
     * When persistent + full eviction + no bloom filters, don't
     * expect to flush.
     *
     * @param expected # of expected items flushed
     * @return # of items expected to flush
     */
    int expectedFlushed(int expected) {
        if (persistent() && fullEviction()) {
            return 0;
        }
        return expected;
    }

    // A set of basic helper functions to reduce the size of tests which are
    // driving a DCPConsumer
    cb::engine_errc snapshot(
            DcpConsumer& consumer,
            uint32_t opaque,
            uint64_t start,
            uint64_t end,
            cb::mcbp::request::DcpSnapshotMarkerFlag flags =
                    cb::mcbp::request::DcpSnapshotMarkerFlag::Memory |
                    cb::mcbp::request::DcpSnapshotMarkerFlag::Checkpoint);
    cb::engine_errc mutation(DcpConsumer& consumer,
                             uint32_t opaque,
                             const DocKeyView& key,
                             uint64_t seqno);
    cb::engine_errc deletion(DcpConsumer& consumer,
                             uint32_t opaque,
                             const DocKeyView& key,
                             uint64_t seqno);
    cb::engine_errc prepare(DcpConsumer& consumer,
                            uint32_t opaque,
                            const DocKeyView& key,
                            uint64_t seqno);
    cb::engine_errc commit(DcpConsumer& consumer,
                           uint32_t opaque,
                           const DocKeyView& key,
                           uint64_t prepareSeqno,
                           uint64_t seqno);
    cb::engine_errc abort(DcpConsumer& consumer,
                          uint32_t opaque,
                          const DocKeyView& key,
                          uint64_t prepareSeqno,
                          uint64_t seqno);
    cb::engine_errc beginCollection(DcpConsumer& consumer,
                                    uint32_t opaque,
                                    uint64_t seqno);

protected:
    void SetUp() override;

    enum class EngineOp : uint8_t { Store, StoreIf, Remove };

    // Test replicating delete times.
    void test_replicateDeleteTime(uint32_t deleteTime);

    /**
     * Verifies that invalid items with empty payload and (datatype != raw) fail
     * validation
     *
     * @param deleted Whether the item under test is alive or deleted
     * @param op The operation under test
     */
    void testValidateDatatypeForEmptyPayload(EngineOp op);

    /**
     * Verifies that checkpoints memory quota threshold is enforced on the given
     * operation.
     *
     * @param op The operation under test
     */

    void testCheckpointMemThresholdEnforced(VbucketOp op);
    /**
     * Test for MB-29512: Check the behaviour of a stream which initially
     * is ahead of purgeSeqno when StreamRequest processed, but by the time
     * the backfill actually occurs compaction has run and advanced the purge
     * seqno so the start seqno is behind it.
     * @param ignorePurgedTombstones If true then use flag
     *        DCP_ADD_STREAM_FLAG_IGNORE_PURGED_TOMBSTONES for streamRequest,
     *        in this case the backfill should succeed.
     */
    void testPurgeSeqnoAdvancesAfterStreamRequest(bool ignorePurgedTombstones);

    MutationStatus public_processSet(VBucket& vb,
                                     Item& item,
                                     const VBQueueItemCtx& ctx = VBQueueItemCtx{
                                             CanDeduplicate::Yes});
};

class STParamPersistentBucketTest : public STParameterizedBucketTest {
protected:
    void testAbortDoesNotIncrementOpsDelete(bool flusherDedup);
    void backfillExpiryOutput(bool xattr);

    /**
     * Test to check that we update and use persistedDeletes correctly.
     *
     * @param dropDeletes compaction config param
     */
    void testCompactionPersistedDeletes(bool dropDeletes);

    void testFailoverTableEntryPersistedAtWarmup(std::function<void()>);

    /**
     * Test that a running compaction is cancelled early after `event` runs.
     *
     * @param event callback executing behaviour which should trigger
     *              cancellation (e.g., shut down engine, delete vb).
     */
    void testCancelCompaction(std::function<void()> event);

    /**
     * Test for MB-51373 - if we end up with a deleted document on-disk with
     * an empty value but datatype=XATTR (when it should be RAW_BYTES - bug
     * MB-52793), then we should sanitize that value when it is loaded from
     * disk.
     * @param fetchMetaOnly If true then when fetching corrupted doc from disk,
     *        only fetch metadata, else fetch entire document.
     */
    void testSanitizeOnDiskDeletedDocWithIncorrectXATTR(bool fetchMetaOnly);

protected:
    EPBucket& getEPBucket();
};
