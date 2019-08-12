/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2016 Couchbase, Inc
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

/*
 * Unit tests for the EPBucket class.
 */

#pragma once

#include "evp_store_test.h"
#include "fakes/fake_executorpool.h"

struct dcp_message_producers;
class MockActiveStreamWithOverloadedRegisterCursor;
class MockDcpMessageProducers;
class MockDcpProducer;

/*
 * A subclass of KVBucketTest which uses a fake ExecutorPool,
 * which will not spawn ExecutorThreads and hence not run any tasks
 * automatically in the background. All tasks must be manually run().
 */
class SingleThreadedKVBucketTest : public KVBucketTest {
public:
    /*
     * Run the next task from the taskQ
     * The task must match the expectedTaskName parameter
     */
    std::chrono::steady_clock::time_point runNextTask(
            TaskQueue& taskQ, const std::string& expectedTaskName);

    /*
     * Run the next task from the taskQ
     */
    std::chrono::steady_clock::time_point runNextTask(TaskQueue& taskQ);

    /*
     * DCP helper. Create a MockDcpProducer configured with (or without)
     * collections and/or delete_times enabled
     * @param cookie cookie to associate with the new producer
     * @param deleteTime yes/no - enable/disable delete times
     */
    std::shared_ptr<MockDcpProducer> createDcpProducer(
            const void* cookie,
            IncludeDeleteTime deleteTime);

    /*
     * DCP helper.
     * Notify and step the given producer
     * @param expectedOp once stepped we expect to see this DCP opcode produced
     * @param fromMemory if false then step a backfill
     */
    void notifyAndStepToCheckpoint(
            MockDcpProducer& producer,
            MockDcpMessageProducers& producers,
            cb::mcbp::ClientOpcode expectedOp =
                    cb::mcbp::ClientOpcode::DcpSnapshotMarker,
            bool fromMemory = true);

    /*
     * DCP helper.
     * Run the active-checkpoint processor task for the given producer
     * @param producer The producer whose task will be ran
     * @param producers The dcp callbacks
     */
    void runCheckpointProcessor(MockDcpProducer& producer,
                                dcp_message_producers& producers);

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
    void createDcpStream(MockDcpProducer& producer, Vbid vbid);

    /**
     * Run the compaction task
     * @param purgeBeforeTime purge tombstones with timestamps less than this
     * @param purgeBeforeSeq purge tombstones with seqnos less than this
     */
    void runCompaction(uint64_t purgeBeforeTime = 0,
                       uint64_t purgeBeforeSeq = 0);

    /**
     * Run the task responsible for iterating the documents and erasing them
     * For persistent buckets integrated into compaction.
     * For ephemeral buckets integrated into stale item removal task
     */
    void runCollectionsEraser();

protected:
    void SetUp() override;

    void TearDown() override;

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

    /*
     * Set the stats isShutdown and attempt to drive all tasks to cancel for
     * the specified engine.
     */
    void shutdownAndPurgeTasks(EventuallyPersistentEngine* ep);

    void cancelAndPurgeTasks();

    /**
     * This method will keep running reader tasks until the engine shows warmup
     * is complete.
     */
    void runReadersUntilWarmedUp();

    /**
     * Destroy engine and replace it with a new engine that can be warmed up.
     */
    void resetEngineAndEnableWarmup(std::string new_config = "");

    /**
     * Destroy engine and replace it with a new engine that can be warmed up.
     * Finally, run warmup.
     */
    void resetEngineAndWarmup(std::string new_config = "");

    /*
     * Fake callback emulating dcp_add_failover_log
     */
    static ENGINE_ERROR_CODE fakeDcpAddFailoverLog(
            vbucket_failover_t* entry,
            size_t nentries,
            gsl::not_null<const void*> cookie) {
        return ENGINE_SUCCESS;
    }

    SingleThreadedExecutorPool* task_executor;
};

/**
 * Test fixture for single-threaded tests on EPBucket.
 */
class SingleThreadedEPBucketTest : public SingleThreadedKVBucketTest {
public:
    enum class BackfillBufferLimit { StreamByte, StreamItem, ConnectionByte };

    void backfillExpiryOutput(bool xattr);
    void producerReadyQLimitOnBackfill(BackfillBufferLimit limitType);

protected:
    EPBucket& getEPBucket() {
        return dynamic_cast<EPBucket&>(*store);
    }
};

/**
 * Test fixture for KVBucket tests running in single-threaded mode, for some
 * combination of bucket type, eviction mode and KVStore type.
 *
 * Allows tests to be defined once which are applicable to more than one
 * configuration, and then instantiated with appropriate config parameters.
 *
 * Parameterised on a pair of:
 * - bucket type (ephemeral or persistent, and additional persistent variants
 *   (e.g. RocksDB) for additional storage backends.
 * - eviction type.
 *   - For ephemeral buckets: used for specifying ephemeral auto-delete /
 *     fail_new_data
 *   - For persistent buckets: used for specifying value_only or full_eviction
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
 *     INSTANTIATE_TEST_CASE_P(
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
 *     INSTANTIATE_TEST_CASE_P(
 *         PersistentAndEphemeral,
 *         DcpActiveStreamTest,
 *         STParameterizedBucketTest::allConfigValues(),
 *         STParameterizedBucketTest::PrintToStringParamName);
 *
 *     INSTANTIATE_TEST_CASE_P(
 *         Persistent,
 *         DcpActiveStreamTestPersistent,
 *         STParameterizedBucketTest::persistentAllBackendsConfigValues(),
 *         STParameterizedBucketTest::PrintToStringParamName());
 */
class STParameterizedBucketTest
    : virtual public SingleThreadedKVBucketTest,
      public ::testing::WithParamInterface<
              std::tuple<std::string, std::string>> {
public:
    static auto ephConfigValues() {
        using namespace std::string_literals;
        return ::testing::Values(
                std::make_tuple("ephemeral"s, "auto_delete"s),
                std::make_tuple("ephemeral"s, "fail_new_data"s));
    }

    static auto allConfigValues() {
        using namespace std::string_literals;
        return ::testing::Values(
                std::make_tuple("ephemeral"s, "auto_delete"s),
                std::make_tuple("ephemeral"s, "fail_new_data"),
                std::make_tuple("persistent"s, "value_only"s),
                std::make_tuple("persistent"s, "full_eviction"s));
    }

    static auto persistentConfigValues() {
        using namespace std::string_literals;
        return ::testing::Values(
                std::make_tuple("persistent"s, "value_only"s),
                std::make_tuple("persistent"s, "full_eviction"s));
    }

    static auto persistentAllBackendsConfigValues() {
        using namespace std::string_literals;
        return ::testing::Values(
                std::make_tuple("persistent"s, "value_only"s),
                std::make_tuple("persistent"s, "full_eviction"s)
#ifdef EP_USE_ROCKSDB
                ,std::make_tuple("persistentRocksdb"s, "value_only"s),
                std::make_tuple("persistentRocksdb"s, "full_eviction"s)
#endif
        );
    }

    bool persistent() const {
        return std::get<0>(GetParam()).find("persistent") != std::string::npos;
    }

    bool fullEviction() const {
        return persistent() && std::get<1>(GetParam()) == "full_eviction";
    }

    /// @returns a string representing this tests' parameters.
    static std::string PrintToStringParamName(
            const ::testing::TestParamInfo<ParamType>& info);

protected:
    void SetUp() override {
        if (!config_string.empty()) {
            config_string += ";";
        }
        auto bucketType = std::get<0>(GetParam());
        if (bucketType == "persistentRocksdb") {
            config_string += "bucket_type=persistent;backend=rocksdb";
        } else {
            config_string += "bucket_type=" + bucketType;
        }
        auto evictionPolicy = std::get<1>(GetParam());

        if (!evictionPolicy.empty()) {
            if (persistent()) {
                config_string += ";item_eviction_policy=" + evictionPolicy;
            } else {
                config_string += ";ephemeral_full_policy=" + evictionPolicy;
            }
        }

        SingleThreadedKVBucketTest::SetUp();
    }
};
